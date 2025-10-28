from __future__ import annotations
import os
from dataclasses import dataclass
from functools import lru_cache
from typing import List


def _parse_int(name: str, default: int) -> int:
    v = os.getenv(name)
    if v is None or v == "":
        return default
    try:
        return int(v)
    except ValueError as e:
        raise ValueError(f"Invalid int for {name}: {v!r}") from e


def _parse_list(name: str) -> List[str]:
    raw = os.getenv(name, "")
    return [x.strip() for x in raw.split(",") if x.strip()]


@dataclass(frozen=True)
class Settings:
    # Identity & network
    node_id: str
    http_port: int
    peers: List[str]

    # Storage & queue/cache
    redis_url: str
    cache_max_items: int
    visibility_timeout_default: int

    # Raft timing
    election_timeout_ms_min: int
    election_timeout_ms_max: int
    heartbeat_ms: int

    # Misc
    log_level: str = "INFO"

    @property
    def is_clustered(self) -> bool:
        return len(self.peers) > 0


@lru_cache(maxsize=1)
def load_settings() -> Settings:
    """
    Membaca environment variables dan mengembalikan Settings (cached).
    """
    return Settings(
        node_id=os.getenv("NODE_ID", "node-1"),
        http_port=_parse_int("HTTP_PORT", 8080),
        peers=_parse_list("PEERS"),  # contoh: "node2:8081,node3:8082"
        redis_url=os.getenv("REDIS_URL", "redis://redis:6379/0"),
        cache_max_items=_parse_int("CACHE_MAX_ITEMS", 10000),
        visibility_timeout_default=_parse_int("VISIBILITY_TIMEOUT_DEFAULT", 30),
        election_timeout_ms_min=_parse_int("ELECTION_TIMEOUT_MS_MIN", 900),
        election_timeout_ms_max=_parse_int("ELECTION_TIMEOUT_MS_MAX", 1500),
        heartbeat_ms=_parse_int("HEARTBEAT_MS", 200),
        log_level=os.getenv("LOG_LEVEL", "INFO"),
    )
