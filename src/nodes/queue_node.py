from __future__ import annotations
import time, json, hashlib
from typing import Dict, Any, List
from redis.asyncio import Redis

# --- HRW hashing inline (tanpa file terpisah) ---
class RendezvousHash:
    def __init__(self, nodes: List[str]):
        self.nodes = list(nodes)

    def set_nodes(self, nodes: List[str]):
        self.nodes = list(nodes)

    def get_node(self, key: str) -> str:
        if not self.nodes:
            raise RuntimeError("No nodes configured for RendezvousHash")
        best_node = None
        best_score = -1
        for n in self.nodes:
            score = int.from_bytes(hashlib.md5(f"{key}|{n}".encode()).digest(), "big")
            if score > best_score:
                best_score = score
                best_node = n
        return best_node  # type: ignore

# --- Distributed Queue ---
class DistQueue:
    """
    Skema kunci Redis per (partition, topic):
      - ready (LIST):   dq:{part}:{topic}:ready
        item = JSON {"msg_id": "...", "payload": {...}}

      - inflight (HASH, baru): dq:{part}:{topic}:inflight
        field = msg_id
        value = JSON {"payload": {...}, "expire_at": <float epoch>}

    Backward-compat:
      - Jika masih ada inflight lama (field = expire_ts, value mengandung msg_id),
        ack() dan requeue_expired() punya fallback untuk menangani format itu.
    """

    def __init__(self, redis: Redis, nodes: List[str]):
        self.redis = redis
        self.hrw = RendezvousHash(nodes)

    def _partition_for(self, topic: str) -> str:
        return self.hrw.get_node(topic)

    async def produce(self, topic: str, payload: Dict[str, Any]) -> str:
        """
        msg_id deterministik dari payload. Ganti ke uuid4().hex jika ingin selalu unik.
        """
        part = self._partition_for(topic)
        ready_key = f"dq:{part}:{topic}:ready"
        msg_id = hashlib.md5(json.dumps(payload, sort_keys=True).encode()).hexdigest()

        msg_obj = {"msg_id": msg_id, "payload": payload}
        await self.redis.rpush(ready_key, json.dumps(msg_obj))
        return msg_id

    async def consume(self, topic: str, max_n: int = 10, visibility_ttl: int = 30) -> List[Dict[str, Any]]:
        """
        Pindah sampai max_n item dari ready -> inflight dengan TTL visibilitas.
        Return list normal: {"msg_id": "...", "payload": {...}}
        """
        part = self._partition_for(topic)
        ready_key = f"dq:{part}:{topic}:ready"
        inflight_key = f"dq:{part}:{topic}:inflight"

        out: List[Dict[str, Any]] = []
        for _ in range(max_n):
            raw = await self.redis.lpop(ready_key)
            if not raw:
                break

            try:
                obj = json.loads(raw)
            except Exception:
                # jika korup, skip (di produksi: catat log/metrics)
                continue

            mid = obj.get("msg_id") or obj.get("id")
            payload = obj.get("payload")

            if not mid:
                # tanpa id tidak bisa di-ACK → kembalikan ke ready agar tidak hilang
                await self.redis.lpush(ready_key, raw)
                continue

            expire_ts = time.time() + visibility_ttl
            inflight_val = {"payload": payload, "expire_at": expire_ts}

            # Skema baru: field=msg_id → O(1) untuk ACK
            await self.redis.hset(inflight_key, mid, json.dumps(inflight_val))

            # Kembalikan ke klien (kalau mau, bisa ikutkan visible_until=expire_ts)
            out.append({"msg_id": mid, "payload": payload})
        return out

    async def ack(self, topic: str, msg_id: str) -> bool:
        """
        ACK O(1): HDEL inflight[msg_id]. Fallback: scan format lama jika belum migrasi.
        """
        part = self._partition_for(topic)
        inflight_key = f"dq:{part}:{topic}:inflight"

        # Coba skema baru terlebih dahulu
        removed = await self.redis.hdel(inflight_key, msg_id)
        if removed == 1:
            return True

        # Fallback: scan skema lama (field = expire_ts, value mengandung msg_id)
        fields = await self.redis.hkeys(inflight_key)
        for f in fields:
            v = await self.redis.hget(inflight_key, f)
            if not v:
                continue
            try:
                obj = json.loads(v)
            except Exception:
                continue

            # format lama bisa {"msg_id": "...", "payload": {...}} tanpa expire_at
            old_mid = obj.get("msg_id") or obj.get("id")
            if old_mid == msg_id:
                await self.redis.hdel(inflight_key, f)
                return True

        return False

    async def requeue_expired(self, topic: str):
        """
        Kembalikan semua inflight yang expire_at <= now ke ready.
        Menangani dua format:
          - baru: field=msg_id, value={"payload":..., "expire_at":...}
          - lama: field=expire_ts, value={"msg_id":..., "payload":...}
        """
        part = self._partition_for(topic)
        inflight_key = f"dq:{part}:{topic}:inflight"
        ready_key = f"dq:{part}:{topic}:ready"
        now = time.time()

        # Ambil seluruh inflight (cukup untuk demo; produksi pakai HSCAN)
        items = await self.redis.hgetall(inflight_key)

        for field, val in items.items():
            try:
                obj = json.loads(val)
            except Exception:
                # tidak bisa parse → buang saja
                await self.redis.hdel(inflight_key, field)
                continue

            # Skema baru?
            expire_at = obj.get("expire_at")
            payload = obj.get("payload")
            if isinstance(expire_at, (int, float)):
                if expire_at <= now:
                    # Requeue ke ready dalam format standard
                    mid = field  # karena field = msg_id
                    msg_obj = {"msg_id": mid, "payload": payload}
                    await self.redis.hdel(inflight_key, field)
                    await self.redis.lpush(ready_key, json.dumps(msg_obj))
                continue

            # Skema lama? field=expire_ts, value berisi msg_id & payload
            mid_old = obj.get("msg_id") or obj.get("id")
            if mid_old is None:
                await self.redis.hdel(inflight_key, field)
                continue

            try:
                expire_ts_old = float(field)
            except Exception:
                # field bukan timestamp → anggap kadaluarsa; migrasikan ke baru
                msg_obj = {"msg_id": mid_old, "payload": payload}
                await self.redis.hdel(inflight_key, field)
                await self.redis.lpush(ready_key, json.dumps(msg_obj))
                continue

            if expire_ts_old <= now:
                # pindahkan ke ready
                msg_obj = {"msg_id": mid_old, "payload": payload}
                await self.redis.hdel(inflight_key, field)
                await self.redis.lpush(ready_key, json.dumps(msg_obj))
