from prometheus_client import Counter, Histogram, Gauge
locks_granted = Counter('locks_granted_total', 'Locks granted')
locks_waiting = Gauge('locks_waiting', 'Locks waiting')
queue_produced = Counter('queue_produced_total', 'Messages produced')
queue_requeued = Counter('queue_requeued_total', 'Requeued due to timeout')
cache_hits = Counter('cache_hits_total', 'Cache hits')
cache_misses = Counter('cache_misses_total', 'Cache misses')
latency = Histogram('http_latency_seconds', 'HTTP latency')
