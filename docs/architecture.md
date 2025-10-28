# Topologi dan Komunikasi Sistem
      [Client] ─HTTP─> [Leader Node] <─── Raft AppendEntries / Heartbeat ───> [Follower]
            \                                          │                              
             \─(queue ops)─> [Shard Node (HRW)] ── Redis (ready/inflight) ─> Requeue/ACK

- Client → Leader: semua mutasi lock diarahkan ke leader. Client/benchmark mendeteksi 409 not_leader dan follow ke alamat leader (hint) atau probe /raft/health.
- Client → Shard (Queue): operasi queue (produce/consume/ack) dapat langsung menuju node hasil HRW hashing atas topic, sementara data ready/inflight disimpan di Redis agar requeue berbasis visibility TTL berjalan deterministik.
- Inter-node: protokol Raft (replication/commit) dan invalidate RPC (cache).
- Observability: /health, /raft/health, failure detector snapshot, serta OpenAPI untuk kontrak antarmuka.
