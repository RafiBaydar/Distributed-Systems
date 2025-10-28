# Distributed Log Aggregator & Queue (Raft + DLM + Redis)

A teaching/assignment project that implements a small distributed system consisting of:
- Raft consensus (single leader, replicated log, majority commit)
- Distributed Lock Manager (DLM) with shared/exclusive modes, wait-queues, and deadlock checks
- Distributed Queue with at-least-once delivery using visibility TTL (requeue on timeout)
- Cache invalidation (“MESI-lite” best-effort)
- Failure detector (peer health & RTT)
- Docker Compose deployment (3 nodes + Redis)
- Benchmarks (round-trip queue, requeue, lock), unit/integration tests

## Quick Start
1. Prereqs
- Docker & Docker Compose
- Python 3.11+ (for running tests/benchmarks locally)
- Windows users: PowerShell available; Linux/macOS users: Bash/zsh

2. Run with Docker Compose

        # From repo root
        docker compose -f docker/docker-compose.yml up --build
This brings up:
- redis:6379
- node1:8080, node2:8081, node3:8082
- Each node gets env vars: NODE_ID, HTTP_PORT, PEERS (service names!), REDIS_URL.

## Find the Leader
# PowerShell (recommended)
    function Get-Leader {
      param([int[]]$Ports=@(8080,8081,8082))
      foreach($p in $Ports){
        try {
          $r = Invoke-RestMethod "http://localhost:$p/raft/health" -TimeoutSec 2
          if($r.state -eq 'leader'){ return "http://localhost:$p" }
        } catch {}
      }
      throw "Leader belum ketemu. Pastikan minimal 2 node UP."
    }
    
    $L = Get-Leader
    "Leader: $L"

## Youtube Link
https://youtu.be/qHgiv9wLmqY
