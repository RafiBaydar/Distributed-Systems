# benchmarks/load_test_scenario.py
from __future__ import annotations
import asyncio
import random
import time
import argparse
import sys
from typing import Dict, List, Tuple
from aiohttp import ClientSession, ClientError

# ---------- util ----------
def pct(sorted_list: List[float], p: float) -> float:
    if not sorted_list: return 0.0
    k = max(0, min(len(sorted_list) - 1, int(round(p * (len(sorted_list) - 1)))))
    return sorted_list[k]

class OpStats:
    def __init__(self, name: str):
        self.name = name
        self.lat_ms: List[float] = []
        self.ok = 0
        self.err = 0
    def add(self, dt: float, ok: bool):
        if ok:
            self.ok += 1
            self.lat_ms.append(dt * 1000.0)
        else:
            self.err += 1
    def summarize(self) -> Dict[str, float]:
        arr = sorted(self.lat_ms)
        return {
            "ok": self.ok,
            "err": self.err,
            "throughput_rps": self.ok / max(1.0, duration),
            "p50_ms": pct(arr, 0.50),
            "p95_ms": pct(arr, 0.95),
            "p99_ms": pct(arr, 0.99),
        }

# ---------- operations ----------
async def op_lock(sess: ClientSession, base: str, stats: OpStats, owner: str):
    rsrc = f"bench-res-{random.randint(1, 256)}"
    t0 = time.perf_counter()
    try:
        # acquire X
        resp = await sess.post(f"{base}/lock/acquire", json={"resource": rsrc, "owner": owner, "mode": "x"})
        if resp.status != 200:
            stats.add(time.perf_counter() - t0, False); return
        # release
        resp = await sess.post(f"{base}/lock/release", json={"resource": rsrc, "owner": owner})
        ok = resp.status == 200
        stats.add(time.perf_counter() - t0, ok)
    except (ClientError, asyncio.TimeoutError):
        stats.add(time.perf_counter() - t0, False)

async def op_queue_produce(sess: ClientSession, base: str, stats: OpStats, topic: str):
    t0 = time.perf_counter()
    try:
        payload = {"id": random.randint(1, 10_000_000), "v": random.random()}
        resp = await sess.post(f"{base}/queue/produce", json={"topic": topic, "payload": payload})
        ok = resp.status == 200
        stats.add(time.perf_counter() - t0, ok)
    except (ClientError, asyncio.TimeoutError):
        stats.add(time.perf_counter() - t0, False)

async def op_queue_consume_ack(sess: ClientSession, base: str, stats: OpStats, topic: str, vis_ttl: int):
    t0 = time.perf_counter()
    try:
        resp = await sess.get(f"{base}/queue/consume", params={"topic": topic, "max": 1, "visibility_ttl": vis_ttl})
        if resp.status != 200:
            stats.add(time.perf_counter() - t0, False); return
        data = await resp.json()
        msgs = data.get("messages", [])
        ok = True
        if msgs:
            mid = msgs[0].get("id")
            r2 = await sess.post(f"{base}/queue/ack", json={"topic": topic, "msg_id": mid})
            ok = ok and (r2.status == 200)
        stats.add(time.perf_counter() - t0, ok)
    except (ClientError, asyncio.TimeoutError):
        stats.add(time.perf_counter() - t0, False)

async def op_cache_put(sess: ClientSession, base: str, stats: OpStats):
    key = f"orders:{random.randint(1, 5000)}"
    body = {"status": "paid", "ts": time.time()}
    t0 = time.perf_counter()
    try:
        resp = await sess.put(f"{base}/cache/{key}", json=body)
        stats.add(time.perf_counter() - t0, resp.status == 200)
    except (ClientError, asyncio.TimeoutError):
        stats.add(time.perf_counter() - t0, False)

async def op_cache_get(sess: ClientSession, base: str, stats: OpStats):
    key = f"orders:{random.randint(1, 5000)}"
    t0 = time.perf_counter()
    try:
        resp = await sess.get(f"{base}/cache/{key}")
        # 200 hit, 404 miss — dua-duanya dianggap sukses untuk latency
        ok = resp.status in (200, 404)
        stats.add(time.perf_counter() - t0, ok)
    except (ClientError, asyncio.TimeoutError):
        stats.add(time.perf_counter() - t0, False)

# ---------- worker ----------
async def worker(idx: int, base_urls: List[str], end_ts: float, weights: Dict[str, int], vis_ttl: int, stats_map: Dict[str, OpStats]):
    # round-robin base url antar node
    owner = f"wrk-{idx}"
    i = 0
    # flatten weighted ops
    ops = (
        ["lock"] * weights["lock"]
        + ["q_prod"] * weights["q_prod"]
        + ["q_cons"] * weights["q_cons"]
        + ["c_put"] * weights["c_put"]
        + ["c_get"] * weights["c_get"]
    )
    if not ops:
        ops = ["lock"]
    topic = "orders"
    timeout = aio_timeout = 3.0
    async with ClientSession(timeout=None) as sess:
        while time.time() < end_ts:
            base = base_urls[i % len(base_urls)]; i += 1
            op = random.choice(ops)
            if op == "lock":
                await op_lock(sess, base, stats_map["lock"], owner)
            elif op == "q_prod":
                await op_queue_produce(sess, base, stats_map["q_prod"], topic)
            elif op == "q_cons":
                await op_queue_consume_ack(sess, base, stats_map["q_cons"], topic, vis_ttl)
            elif op == "c_put":
                await op_cache_put(sess, base, stats_map["c_put"])
            elif op == "c_get":
                await op_cache_get(sess, base, stats_map["c_get"])

# ---------- main ----------
def parse_args():
    ap = argparse.ArgumentParser(description="Distributed Sync System benchmark")
    ap.add_argument("--base", nargs="+", default=["http://localhost:8080"], help="Base URLs (bisa banyak, spasi dipisah)")
    ap.add_argument("--concurrency", "-c", type=int, default=32, help="Jumlah worker concurrent")
    ap.add_argument("--duration", "-d", type=int, default=30, help="Durasi test (detik)")
    ap.add_argument("--visibility-ttl", type=int, default=15, help="Queue visibility TTL (detik)")
    ap.add_argument("--w-lock", type=int, default=2, help="Bobot operasi lock")
    ap.add_argument("--w-q-produce", type=int, default=2, help="Bobot operasi queue produce")
    ap.add_argument("--w-q-consume", type=int, default=2, help="Bobot operasi queue consume+ack")
    ap.add_argument("--w-cache-put", type=int, default=1, help="Bobot operasi cache put")
    ap.add_argument("--w-cache-get", type=int, default=3, help="Bobot operasi cache get")
    return ap.parse_args()

duration = 0  # diisi di runtime untuk summary

async def amain():
    global duration
    args = parse_args()
    duration = args.duration

    # Windows event loop policy
    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass

    stats_map = {
        "lock": OpStats("lock"),
        "q_prod": OpStats("queue_produce"),
        "q_cons": OpStats("queue_consume_ack"),
        "c_put": OpStats("cache_put"),
        "c_get": OpStats("cache_get"),
    }
    weights = {
        "lock": args.w_lock,
        "q_prod": args.w_q_produce,
        "q_cons": args.w_q_consume,
        "c_put": args.w_cache_put,
        "c_get": args.w_cache_get,
    }

    end_ts = time.time() + args.duration
    tasks = [
        asyncio.create_task(worker(i, args.base, end_ts, weights, args.visibility_ttl, stats_map))
        for i in range(args.concurrency)
    ]
    await asyncio.gather(*tasks, return_exceptions=True)

    # summary
    print("\n=== Benchmark Summary ===")
    total_ok = total_err = 0
    for k, s in stats_map.items():
        sm = s.summarize()
        total_ok += sm["ok"]; total_err += sm["err"]
        print(f"[{s.name}] ok={sm['ok']} err={sm['err']} thr={sm['throughput_rps']:.1f} rps "
              f"p50={sm['p50_ms']:.1f}ms p95={sm['p95_ms']:.1f}ms p99={sm['p99_ms']:.1f}ms")
    print(f"TOTAL ok={total_ok} err={total_err}")

if __name__ == "__main__":
    asyncio.run(amain())
