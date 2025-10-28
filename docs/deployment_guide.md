# DEPLOYMENT GUIDE

## 1. Start Awal
    docker compose -f docker\docker-compose.yml down -v
    docker compose -f docker\docker-compose.yml up -d --build

## 2. Helper untuk cari leader yang aktif (auto-retry)
    $ports = 8080,8081,8082
    foreach($p in $ports){
    try { $h = Invoke-RestMethod "http://localhost:$p/health" -TimeoutSec 2; "UP : $p ($($h.node))" }
    catch { "DOWN: $p" }
    }

    function Get-Leader {
    param([int[]]$Ports = @(8080,8081,8082), [int]$Retries=20, [int]$DelaySec=1)
    for($i=0; $i -lt $Retries; $i++){
        foreach($p in $Ports){
        try{
            $r = Invoke-RestMethod "http://localhost:$p/raft/health" -TimeoutSec 2
            if($r.state -eq 'leader'){ return "http://localhost:$p" }
        } catch {}
        }
        Start-Sleep -Seconds $DelaySec
    }
    throw "Leader belum ketemu. Pastikan minimal 2 node UP."
    }

    $L = Get-Leader
    "Leader: $L"

## 3. Distributed Lock (via leader)
    # Acquire X-lock
    $acq = @{ resource='res-A'; owner='cli-1'; mode='x' } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/lock/acquire" -ContentType 'application/json' -Body $acq

    # Release
    $rel = @{ resource='res-A'; owner='cli-1' } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/lock/release" -ContentType 'application/json' -Body $rel

## 4. Distributed Queue: Produce → Consume → ACK
    # Produce satu pesan
    $body = @{ topic='orders'; payload=@{ id=1; ts=[DateTime]::UtcNow } } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/queue/produce" -ContentType 'application/json' -Body $body

    # Consume (visibility 60s)
    $cons = Invoke-RestMethod "$L/queue/consume?topic=orders&max=1&visibility_ttl=60"
    $mid  = $cons.messages[0].msg_id

    # ACK
    $ack  = @{ topic='orders'; msg_id=$mid } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/queue/ack" -ContentType 'application/json' -Body $ack

## 5. At-least-once (requeue otomatis bila tidak di-ACK)
    # Produce
    $body = @{ topic='bench'; payload=@{ demo='requeue'; ts=[DateTime]::UtcNow } } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/queue/produce" -ContentType 'application/json' -Body $body | Out-Null

    # Consume TANPA ACK (visibility 3s)
    $cons1 = Invoke-RestMethod "$L/queue/consume?topic=bench&max=1&visibility_ttl=3"
    $cons1 | ConvertTo-Json -Depth 6 | Write-Host
    Start-Sleep 4

    # Pesan muncul lagi (requeue)
    $cons2 = Invoke-RestMethod "$L/queue/consume?topic=bench&max=1&visibility_ttl=10"
    $cons2 | ConvertTo-Json -Depth 6 | Write-Host

    # ACK supaya selesai
    $ack2 = @{ topic='bench'; msg_id=$cons2.messages[0].msg_id } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/queue/ack" -ContentType 'application/json' -Body $ack2

## 6. Failover demo (stop leader → pilih leader baru → lanjut jalan)
    # Temukan container nama leader dari port
    $leaderPort = ($L -split ':')[-1]
    docker ps --format "table {{.Names}}\t{{.Ports}}" | findstr $leaderPort

    # STOP container leader (ganti dengan nama container hasil baris di atas)
    # contoh:
    # docker stop docker-node2-1

    # Cari leader baru
    $L = Get-Leader
    "Leader baru: $L"

    # Tetap bisa produce/consume
    $body = @{ topic='failover'; payload=@{ i=1; ts=[DateTime]::UtcNow } } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/queue/produce" -ContentType 'application/json' -Body $body | Out-Null
    Invoke-RestMethod "$L/queue/consume?topic=failover&max=1&visibility_ttl=10" | ConvertTo-Json -Depth 6

## 7. Cache Coherence (invalidation lintas node)
    $N1 = "http://localhost:8080"
    $N3 = "http://localhost:8082"

    # Simpan di node-3
    Invoke-RestMethod -Method PUT "$N3/cache/user:42" -ContentType 'application/json' `
    -Body (@{ name='Rafi-old' } | ConvertTo-Json -Compress)

    # Update di node-1 → broadcast invalidation
    Invoke-RestMethod -Method PUT "$N1/cache/user:42" -ContentType 'application/json' `
    -Body (@{ name='Rafi-new' } | ConvertTo-Json -Compress)

    # GET di node-3 harus MISS (sudah invalidated)
    try { Invoke-RestMethod "$N3/cache/user:42" } catch { "node-3 => MISS (OK)" }

    # GET di node-1 harus value terbaru
    Invoke-RestMethod "$N1/cache/user:42"

## 8. Mini Benchmark (throughput produce & ACK)
    # Produce N pesan & ukur waktu
    $N = 300
    $sw = [Diagnostics.Stopwatch]::StartNew()
    1..$N | %{
    $b = @{ topic='bench'; payload=@{ i=$_; ts=[DateTime]::UtcNow } } | ConvertTo-Json -Compress
    Invoke-RestMethod -Method POST "$L/queue/produce" -ContentType 'application/json' -Body $b | Out-Null
    }
    $sw.Stop()
    "Produce: $N msgs in {0} ms (~{1:n2} msg/s)" -f $sw.ElapsedMilliseconds, ($N/($sw.ElapsedMilliseconds/1000.0))

    # Consume & ACK semuanya
    $cons  = Invoke-RestMethod "$L/queue/consume?topic=bench&max=$N&visibility_ttl=30"
    $ackOk = 0
    foreach($m in $cons.messages){
    $body = @{ topic='bench'; msg_id=$m.msg_id } | ConvertTo-Json -Compress
    $r = Invoke-RestMethod -Method POST "$L/queue/ack" -ContentType 'application/json' -Body $body
    if($r.ack){ $ackOk++ }
    }
    "ACK ok: $ackOk / $($cons.messages.Count)"

## Troubleshooting
    docker compose -f docker\docker-compose.yml ps
    docker logs --tail=80 docker-node1-1
    docker logs --tail=80 docker-node2-1
    docker logs --tail=80 docker-node3-1
    # restart node tertentu jika perlu:
    # docker restart docker-node2-1

## 10. Takedown (opsional setelah rekaman)
    docker compose -f docker\docker-compose.yml down -v
