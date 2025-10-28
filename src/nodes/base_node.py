from __future__ import annotations
import asyncio
import inspect
from typing import Any, Dict, List, Set, Optional

from aiohttp import web, ClientSession
from redis.asyncio import Redis

from src.consensus.raft import RaftNode
from src.nodes.lock_manager import LockTable, LockMode
from src.nodes.queue_node import DistQueue
from src.nodes.cache_node import Directory, CacheNode, MESI
from src.communication.message_passing import JsonRpcRegistry
from src.communication.failure_detector import FailureDetector
from src.utils.config import load_settings

# --------- GLOBALS ---------
lock_table: LockTable
directory: Directory
cache_node: CacheNode
dist_queue: DistQueue
peers_all: List[str]           # semua node (HRW queue & broadcast)
raft_peers: List[str]          # peers RAFT tanpa diri sendiri
node_id: str
my_addr: str                   # "nodeX:port" milik diri sendiri
session: ClientSession
known_topics: Set[str]
fd: Optional[FailureDetector] = None

# --------- Helpers: adapt sync/async ---------
async def maybe_await(x):
    return await x if inspect.isawaitable(x) else x

def apply_command(cmd: Dict[str, Any]):
    t = cmd.get("type")
    if t == "LOCK_ACQUIRE":
        lock_table.acquire(cmd["resource"], cmd["owner"], cmd["mode"])
    elif t == "LOCK_RELEASE":
        lock_table.release(cmd["resource"], cmd["owner"])
    # tambahkan command lain bila perlu

async def apply_adapter(cmd: Dict[str, Any]):
    """Pastikan Raft bisa memanggil apply_fn baik sync maupun async."""
    res = apply_command(cmd)
    if inspect.isawaitable(res):
        return await res
    return res

# --------- BROADCAST INVALIDATION (exclude diri sendiri) ---------
async def _post_json(url: str, payload: Dict[str, Any], timeout: float = 2.5):
    try:
        async with session.post(url, json=payload, timeout=timeout) as resp:
            # baca body agar koneksi dibersihkan
            try:
                await resp.json()
            except Exception:
                await resp.text()
    except Exception:
        pass  # best-effort

async def broadcast_invalidate(key: str):
    tasks = []
    for p in peers_all:
        if p == my_addr:
            continue
        url = f"http://{p}/rpc"
        payload = {"method": "cache.invalidate", "params": {"key": key}}
        tasks.append(asyncio.create_task(_post_json(url, payload)))
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)

# --------- LEADER DISCOVERY (untuk UX lebih baik) ---------
async def find_leader(peers: List[str]) -> Optional[str]:
    # cek diri sendiri dulu
    try:
        async with session.get(f"http://{my_addr}/raft/health", timeout=1.5) as r:
            if r.status == 200:
                data = await r.json()
                if data.get("state") == "leader":
                    return my_addr
    except Exception:
        pass
    # cek peers lain
    for p in peers:
        try:
            async with session.get(f"http://{p}/raft/health", timeout=1.5) as r:
                if r.status == 200:
                    data = await r.json()
                    if data.get("state") == "leader":
                        return p
        except Exception:
            continue
    return None

# --------- MAIN ---------
async def main():
    global lock_table, directory, cache_node, dist_queue, peers_all, raft_peers, node_id, my_addr, session, known_topics, fd

    cfg = load_settings()
    node_id = cfg.node_id
    port = cfg.http_port
    peers_all = cfg.peers                            # contoh: ["node1:8080","node2:8081","node3:8082"]
    redis_url = cfg.redis_url
    cache_max = cfg.cache_max_items

    # alamat diri sendiri & peers Raft
    my_addr = next((p for p in peers_all if p.endswith(f":{port}")), f"{node_id}:{port}")
    raft_peers = [p for p in peers_all if p != my_addr]

    # Komponen lokal
    lock_table = LockTable()
    directory = Directory()
    cache_node = CacheNode(node_id=node_id, directory=directory, max_items=cache_max)
    known_topics = set()

    # Redis & Queue (HRW harus lihat daftar yang sama di semua node)
    redis = Redis.from_url(redis_url, encoding="utf-8", decode_responses=True)
    dist_queue = DistQueue(redis=redis, nodes=peers_all or [my_addr])

    # HTTP client
    session = ClientSession()

    # === 1) Buat RaftNode (BELUM start) ===
    raft = RaftNode(
        node_id=node_id,
        port=port,
        peers=raft_peers,  # penting: tanpa diri sendiri
        apply_fn=apply_adapter,  # <- adapt sync/async
        election_min_ms=cfg.election_timeout_ms_min,
        election_max_ms=cfg.election_timeout_ms_max,
        heartbeat_ms=cfg.heartbeat_ms,
    )

    # Ambil app AioHTTP dari Raft
    app: web.Application = getattr(raft, "app", getattr(raft, "_app", None))
    if app is None:
        raise RuntimeError("RaftNode tidak mengekspos app/_app")

    # ---- Error middleware agar tidak ada 500 generik ----
    @web.middleware
    async def err_middleware(request, handler):
        try:
            return await handler(request)
        except web.HTTPException:
            raise
        except Exception as e:
            print("[unhandled]", repr(e))
            return web.json_response({"error": "internal", "detail": repr(e)}, status=500)

    app.middlewares.append(err_middleware)

    # === 2) DAFTARKAN SEMUA ROUTES SEBELUM start() ===

    # RPC registry (/rpc) untuk invalidasi cache
    rpc = JsonRpcRegistry()

    async def rpc_cache_invalidate(key: str) -> Dict[str, Any]:
        # invalidate lokal + set state I
        try:
            cache_node.cache.od.pop(key, None)  # LRU sederhana
        except Exception:
            pass
        holders = directory.holders.get(key, set())
        if node_id in holders:
            holders.discard(node_id)
            directory.holders[key] = holders
        directory.state[key] = MESI.I
        return {"invalidated": key, "node": node_id}

    rpc.register("cache.invalidate", rpc_cache_invalidate)
    app.add_routes([web.post("/rpc", rpc.handle)])

    # Failure Detector snapshot endpoint
    async def fd_status(_req: web.Request):
        snap = fd.snapshot() if fd else {}
        return web.json_response(snap)
    app.add_routes([web.get("/fd", fd_status)])

    # Health + helper info peers
    async def health(_):
        return web.json_response({
            "ok": True,
            "node": node_id,
            "peers_queue": peers_all,   # semua (HRW queue)
            "peers_raft": raft_peers,   # tanpa diri sendiri
            "raft_state": raft.state,
            "term": raft.term,
        })
    app.add_routes([web.get("/health", health)])

    # Raft leader helper
    async def raft_leader(_):
        leader = await find_leader(peers_all)
        return web.json_response({"leader": leader})
    app.add_routes([web.get("/raft/leader", raft_leader)])

    # --------- Lock endpoints (via Raft) ---------
    async def lock_acquire(req: web.Request):
        body = await req.json()
        resource = body.get("resource"); owner = body.get("owner"); mode = body.get("mode")
        if not resource or not owner or mode not in (LockMode.SHARED, LockMode.EXCLUSIVE):
            return web.json_response({"error": "bad_request"}, status=400)
        if raft.state != "leader":
            leader = await find_leader(peers_all)
            return web.json_response({"error": "not_leader", "leader": leader}, status=409)
        try:
            ok = await maybe_await(raft.propose({"type": "LOCK_ACQUIRE", "resource": resource, "owner": owner, "mode": mode}))
            if ok is False:
                return web.json_response({"error": "commit_failed"}, status=503)
            return web.json_response({"status": "enqueued_or_granted"})
        except web.HTTPException as e:
            return web.json_response({"error": e.reason or "raft_http_error"}, status=e.status)
        except Exception as e:
            print("[lock_acquire] exception:", repr(e))
            return web.json_response({"error": "internal", "detail": "raft_propose_failed"}, status=500)

    async def lock_release(req: web.Request):
        body = await req.json()
        resource = body.get("resource"); owner = body.get("owner")
        if not resource or not owner:
            return web.json_response({"error": "bad_request"}, status=400)
        if raft.state != "leader":
            leader = await find_leader(peers_all)
            return web.json_response({"error": "not_leader", "leader": leader}, status=409)
        try:
            ok = await maybe_await(raft.propose({"type": "LOCK_RELEASE", "resource": resource, "owner": owner}))
            if ok is False:
                return web.json_response({"error": "commit_failed"}, status=503)
            return web.json_response({"status": "released"})
        except web.HTTPException as e:
            return web.json_response({"error": e.reason or "raft_http_error"}, status=e.status)
        except Exception as e:
            print("[lock_release] exception:", repr(e))
            return web.json_response({"error": "internal", "detail": "raft_propose_failed"}, status=500)

    app.add_routes([
        web.post("/lock/acquire", lock_acquire),
        web.post("/lock/release", lock_release),
    ])

    # --------- Queue endpoints ---------
    async def queue_produce(req: web.Request):
        body = await req.json()
        topic = body.get("topic"); payload = body.get("payload")
        if not topic or payload is None:
            return web.json_response({"error": "missing topic/payload"}, status=400)
        msg_id = await dist_queue.produce(topic, payload)
        known_topics.add(topic)
        return web.json_response({"produced": True, "msg_id": msg_id})

    async def queue_consume(req: web.Request):
        topic = req.query.get("topic")
        if not topic:
            return web.json_response({"error": "missing topic"}, status=400)
        max_n = int(req.query.get("max", "10"))
        vis = int(req.query.get("visibility_ttl", cfg.visibility_timeout_default))
        msgs = await dist_queue.consume(topic, max_n=max_n, visibility_ttl=vis)
        return web.json_response({"messages": msgs})

    async def queue_ack(req: web.Request):
        body = await req.json()
        topic = body.get("topic"); msg_id = body.get("msg_id")
        if not topic or not msg_id:
            return web.json_response({"error": "missing topic/msg_id"}, status=400)
        ok = await dist_queue.ack(topic, msg_id)
        return web.json_response({"ack": ok})

    app.add_routes([
        web.post("/queue/produce", queue_produce),
        web.get("/queue/consume", queue_consume),
        web.post("/queue/ack", queue_ack),
    ])

    # --------- Cache endpoints ---------
    async def cache_get(req: web.Request):
        key = req.match_info["key"]
        val = cache_node.get(key)
        if val is None:
            return web.json_response({"hit": False}, status=404)
        return web.json_response({"hit": True, "value": val})

    async def cache_put(req: web.Request):
        key = req.match_info["key"]
        try:
            body = await req.json()
        except Exception:
            return web.json_response({"error": "invalid_json"}, status=400)
        cache_node.put(key, body)          # local write → M
        asyncio.create_task(broadcast_invalidate(key))  # invalidate others
        return web.json_response({"stored": True, "key": key})

    app.add_routes([
        web.get("/cache/{key}", cache_get),
        web.put("/cache/{key}", cache_put),
    ])

    # === 3) Setelah SEMUA routes terpasang, baru start server Raft ===
    await raft.start()

    # === 4) Start Failure Detector & background requeue ===
    fd = FailureDetector(peers=peers_all, path="/health", interval=1.0, timeout=0.7, failure_window=3.0)
    fd.subscribe(lambda peer, up, rtt: print(f"[FD] {peer} => {'UP' if up else 'DOWN'} rtt={rtt:.3f}s"))
    await fd.start()

    async def requeue_daemon():
        while True:
            try:
                for t in list(known_topics):
                    await dist_queue.requeue_expired(t)
            except Exception as e:
                print("[requeue_daemon] error:", e)
            await asyncio.sleep(1.0)

    requeue_task = asyncio.create_task(requeue_daemon())

    try:
        await asyncio.Event().wait()
    finally:
        requeue_task.cancel()
        try:
            await requeue_task
        except asyncio.CancelledError:
            pass
        await fd.stop()
        await session.close()

if __name__ == "__main__":
    asyncio.run(main())
