[Client] ─HTTP─> [Leader Node] <─── Raft AppendEntries / Heartbeat ───> [Follower]
      \                                          │                              
       \─(queue ops)─> [Shard Node (HRW)] ── Redis (ready/inflight) ─> Requeue/ACK
