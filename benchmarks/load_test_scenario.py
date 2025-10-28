# benchmarks/load_test_scenario.py (VERSI OPTIMAL & EFISIEN)
from __future__ import annotations

import argparse
import asyncio
import json
import random
import time
from dataclasses import dataclass, field
from typing import Dict, List, Tuple, Optional
from urllib.parse import urlparse, urljoin

# --- PERUBAHAN 1: Tambahkan TCPConnector ---
from aiohttp import ClientSession, ClientError, TCPConnector

# ===================== Stats helpers =====================
# ... (Tidak ada perubahan di bagian ini) ...
@dataclass
class OpStats:
    n: int = 0
    ok: int = 0
    lat_ms: List[float] = field(default_factory=list)

    def add(self, ok: bool, lat_ms: float) -> None:
        self.n += 1
        if ok:
            self.ok += 1
        self.lat_ms.append(lat_ms)

    def avg_ms(self) -> float:
        return (sum(self.lat_ms) / len(self.lat_ms)) if self.lat_ms else 0.0


@dataclass
class BenchStats:
    ops: Dict[str, OpStats] = field(default_factory=dict)
    t_start: float = 0.0
    t_end: float = 0.0

    def rec(self, op: str, ok: bool, lat_ms: float) -> None:
        self.ops.setdefault(op, OpStats()).add(ok, lat_ms)

    @property
    def total_req(self) -> int: return sum(s.n for s in self.ops.values())
    @property
    def total_ok(self)  -> int: return sum(s.ok for s in self.ops.values())
    @property
    def total_fail(self)-> int: return self.total_req - self.total_ok
    @property
    def duration(self)  -> float: return max(0.0, self.t_end - self.t_start)
    @property
    def avg_latency(self)-> float:
        all_lat = [lm for s in self.ops.values() for lm in s.lat_ms]
        return (sum(all_lat) / len(all_lat)) if all_lat else 0.0
    @property
    def throughput(self) -> float:
        return (self.total_req / self.duration) if self.duration > 0 else 0.0

# ===================== IO helpers =====================
# ... (Tidak ada perubahan di bagian ini) ...
def line(ch: str = "-") -> None:
    print(ch * 74)

def _safe_json(txt: str) -> Dict:
    try:
        return json.loads(txt)
    except Exception:
        return {"text": txt[:500]}

async def timed_request_full(
    sess: ClientSession, method: str, url: str, *, json_body=None, timeout: float = 5.0
) -> Tuple[bool, float, Dict, int, Dict[str,str]]:
    t0 = time.perf_counter()
    ok = False
    data: Dict = {}
    status = 0
    headers: Dict[str,str] = {}
    try:
        async with sess.request(method, url, json=json_body, timeout=timeout, allow_redirects=False) as resp:
            status = resp.status
            headers = {k.lower(): v for k, v in resp.headers.items()}
            ct = headers.get("content-type", "")
            if "application/json" in ct.lower():
                data = await resp.json()
            else:
                txt = await resp.text()
                data = _safe_json(txt)
            ok = 200 <= status < 300 and not (isinstance(data, dict) and data.get("error"))
    except ClientError as e:
        data = {"error": str(e)}
        ok = False
    lat_ms = (time.perf_counter() - t0) * 1000.0
    return ok, lat_ms, data, status, headers

async def timed_request(
    sess: ClientSession, method: str, url: str, *, json_body=None, timeout: float = 5.0
) -> Tuple[bool, float, Dict]:
    ok, lat, data, _, _ = await timed_request_full(sess, method, url, json_body=json_body, timeout=timeout)
    return ok, lat, data

async def run_pool(total: int, conc: int, worker):
    q: asyncio.Queue[int] = asyncio.Queue()
    for i in range(total):
        q.put_nowait(i)

    async def consumer():
        while not q.empty():
            try:
                idx = q.get_nowait()
            except asyncio.QueueEmpty:
                return
            try:
                await worker(idx)
            finally:
                q.task_done()

    tasks = [asyncio.create_task(consumer()) for _ in range(conc)]
    await asyncio.gather(*tasks)

# ===================== Leader cache =====================
# ... (Tidak ada perubahan di bagian ini) ...
class LeaderRef:
    def __init__(self, initial: str):
        self._url = initial.rstrip("/")
        self._mtx = asyncio.Lock()
        self.last_switch: float = 0.0
    async def get(self) -> str:
        return self._url
    async def set(self, url: str) -> None:
        async with self._mtx:
            if self._url != url.rstrip("/"):
                self._url = url.rstrip("/")
    async def has_recently_switched(self, threshold: float = 0.5) -> bool:
        return (time.perf_counter() - self.last_switch) < threshold

# ===================== Leader probing & mapping =====================
# ... (Tidak ada perubahan di bagian ini) ...
def is_not_leader(status: int, data: Dict, headers: Dict[str,str]) -> bool:
    if status == 409:
        return True
    if status in (301, 302, 307, 308) and ("location" in headers):
        return True
    if headers.get("x-raft-leader") or headers.get("x-leader") or headers.get("raft-leader"):
        return True
    if isinstance(data, dict):
        err = str(data.get("error", "")).lower()
        if "not_leader" in err or "not leader" in err or data.get("leader"):
            return True
    if status == 503 and headers.get("x-raft-role","").lower() == "follower":
        return True
    return False

def is_transient_error(status: int, data: Dict) -> bool:
    if status in (409, 503):
        return True
    if isinstance(data, dict):
        err = str(data.get("error", "")).lower()
        if "commit_failed" in err or "not_leader" in err or "unavailable" in err:
            return True
    return False

def parse_nodes_arg(nodes_arg: str) -> List[str]:
    return [s.strip().rstrip("/") for s in (nodes_arg or "").split(",") if s.strip()]

async def probe_leader(sess: ClientSession, nodes: List[str], timeout: float = 2.0) -> Optional[str]:
    async def one(base):
        ok, _, data, _, _ = await timed_request_full(
            sess, "GET", f"{base.rstrip('/')}/raft/health", timeout=timeout
        )
        if ok and isinstance(data, dict) and data.get("state") == "leader":
            return base.rstrip("/")
        return None
    tasks = [asyncio.create_task(one(n)) for n in nodes]
    for fut in asyncio.as_completed(tasks):
        res = await fut
        if res:
            for t in tasks:
                if not t.done():
                    t.cancel()
            return res
    return None

def map_hint_to_url(hint: str, nodes: List[str]) -> Optional[str]:
    try:
        s = hint.strip()
        if "://" not in s:
            s = "http://" + s
        parsed = urlparse(s)
        port = parsed.port
        host = parsed.hostname or "localhost"
        if port:
            for n in nodes:
                pn = urlparse(n)
                if pn.port == port:
                    return f"{pn.scheme}://{pn.hostname}:{pn.port}"
            return f"http://localhost:{port}"
        return f"{parsed.scheme}://{host}".rstrip("/")
    except Exception:
        return None

async def discover_nodes_if_empty(sess: ClientSession, nodes: List[str]) -> List[str]:
    if nodes:
        return nodes
    candidates = [f"http://localhost:{p}" for p in range(8080, 8086)]
    found: List[str] = []
    async def probe(u):
        ok, _, _, _, _ = await timed_request_full(sess, "GET", f"{u}/raft/health", timeout=0.8)
        return u if ok else None
    tasks = [asyncio.create_task(probe(u)) for u in candidates]
    for fut in asyncio.as_completed(tasks):
        r = await fut
        if r: found.append(r)
    return found or candidates

# ===================== Hedged POST (race) =====================
# ... (Tidak ada perubahan di bagian ini) ...
async def race_post_to_nodes(
    sess: ClientSession, nodes: List[str], path: str, body: Dict, timeout: float
) -> Tuple[bool, float, Dict, int, Optional[str]]:
    async def one(node):
        ok, lat, data, status, _ = await timed_request_full(
            sess, "POST", f"{node.rstrip('/')}{path}", json_body=body, timeout=timeout
        )
        if ok or is_not_leader(status, data, {}):
             return ok, lat, data, status, node
        return False, lat, data, status, None

    tasks = [asyncio.create_task(one(n)) for n in nodes]
    
    first: Optional[Tuple[bool, float, Dict, int, Optional[str]]] = None

    try:
        for fut in asyncio.as_completed(tasks):
            res = await fut
            if res is None: continue
            
            ok, lat, data, status, node = res

            if ok:
                first = (ok, lat, data, status, node)
                break
            
            if first is None:
                first = (ok, lat, data, status, node)
    finally:
        for t in tasks:
            if not t.done():
                t.cancel()
    
    if first is None:
        return False, 0.0, {"error": "no attempt finished"}, 0, None
    
    return first

# ===================== Grant helpers =====================
# ... (Tidak ada perubahan di bagian ini) ...
async def is_granted(sess: ClientSession, base: str, resource: str, owner: str, timeout: float = 2.0) -> bool:
    candidates = [
        f"{base}/lock/state?resource={resource}",
        f"{base}/locks/state?resource={resource}",
        f"{base}/lock/state/{resource}",
    ]
    for url in candidates:
        ok, _, data, _, _ = await timed_request_full(sess, "GET", url, timeout=timeout)
        if not ok:
            continue
        if isinstance(data, dict):
            holder = data.get("holder") or data.get("owner")
            status_s = (data.get("status") or "").lower()
            granted_b = bool(data.get("granted", False))
            if holder == owner and (granted_b or status_s in {"granted", "held"}):
                return True
    return False

async def wait_until_granted(
    sess: ClientSession, base: str, resource: str, owner: str, max_wait_ms: int
) -> None:
    t0 = time.perf_counter()
    budget = max_wait_ms / 1000.0
    while time.perf_counter() - t0 < budget:
        if await is_granted(sess, base, resource, owner, timeout=0.5):
            return
        await asyncio.sleep(0.005)
    await asyncio.sleep(0.01)

# ===================== New Resilient POST Logic (REFACTORED) =====================
# ... (Tidak ada perubahan di bagian ini) ...
async def _post_with_retry_and_follow(
    args, sess: ClientSession, leader_ref: LeaderRef, nodes: List[str], path: str, body: Dict
) -> Tuple[bool, float, str, Optional[Tuple[int, Dict]]]:
    max_retries = 5 
    lat_sum = 0.0
    err_details: Optional[Tuple[int, Dict]] = None
    last_base_used = await leader_ref.get()

    for attempt in range(max_retries):
        base = await leader_ref.get()
        
        ok, lat, data, status, headers = await timed_request_full(
            sess, "POST", f"{base}{path}", json_body=body, timeout=args.timeout
        )
        lat_sum += lat
        
        if ok and not is_not_leader(status, data, headers):
            return True, lat_sum, base, None

        is_not_ldr = is_not_leader(status, data, headers)
        is_trans = is_transient_error(status, data)
        err_details = (status, data if isinstance(data, dict) else {"body": data})
        last_base_used = base

        new_base: Optional[str] = None
        
        loc = headers.get("location")
        if loc:
            new_base = urljoin(base + "/", loc).rstrip("/").rsplit("/", 1)[0]
        if not new_base:
            for hk in ("x-raft-leader", "x-leader", "raft-leader"):
                if headers.get(hk):
                    new_base = map_hint_to_url(headers[hk], nodes)
                    break
        if not new_base and isinstance(data, dict) and data.get("leader"):
            new_base = map_hint_to_url(str(data["leader"]), nodes)

        if new_base and new_base.rstrip("/") != base:
            await leader_ref.set(new_base)
            continue

        if is_not_ldr:
            ok_race, lat_race, _, _, winner = await race_post_to_nodes(sess, nodes, path, body, args.timeout)
            lat_sum += lat_race
            if ok_race and winner:
                await leader_ref.set(winner)
                return True, lat_sum, winner, None
            if winner:
                 await leader_ref.set(winner)

        if is_trans:
            backoff_sec = min(args.timeout / 3.0, (2 ** attempt) * 0.05)
            jitter = random.uniform(0.0, backoff_sec)
            await asyncio.sleep(backoff_sec + jitter)
            continue

        break

    return False, lat_sum, last_base_used, err_details

# ===================== Scenarios =====================

async def scenario_lock(args, stats: BenchStats):
    """
    acquire -> (optional wait for grant) -> release
    """
    raw_nodes = parse_nodes_arg(args.nodes)
    leader_ref = LeaderRef(args.leader)

    # --- PERUBAHAN 2: Tambahkan TCPConnector ---
    # Optimasi: Naikkan limit koneksi agar sesuai/lebih besar dari concurrency
    conn_limit = max(100, args.concurrency * 2)
    conn = TCPConnector(limit_per_host=conn_limit)

    async with ClientSession(connector=conn) as sess:
        nodes = await discover_nodes_if_empty(sess, raw_nodes)

        snap = await probe_leader(sess, nodes)
        if snap:
            await leader_ref.set(snap)

        err_samples: List[Tuple[int,Dict]] = []

        async def worker(i: int):
            owner = f"cli-{i}"
            res = f"res-{i}" 

            ok, lat, base_used, err = await _post_with_retry_and_follow(
                args, sess, leader_ref, nodes, "/lock/acquire", {"resource": res, "owner": owner, "mode": "x"}
            )
            stats.rec("lock.acquire", ok, lat)
            if (not ok) and err and len(err_samples) < 5 and args.verbose:
                err_samples.append(err)

            if ok and args.grant_wait_ms > 0:
                await wait_until_granted(sess, base_used, res, owner, max_wait_ms=args.grant_wait_ms)

            ok2, lat2, _, err2 = await _post_with_retry_and_follow(
                args, sess, leader_ref, nodes, "/lock/release", {"resource": res, "owner": owner}
            )
            stats.rec("lock.release", ok2, lat2)
            if (not ok2) and err2 and len(err_samples) < 5 and args.verbose:
                err_samples.append(err2)

        await run_pool(args.iterations, args.concurrency, worker)

        if args.verbose and err_samples:
            print("[DEBUG] Sample errors (status, body) →")
            for st, body in err_samples:
                print(f"  - {st}: {json.dumps(body)[:300]}")

async def scenario_roundtrip(args, stats: BenchStats):
    base = args.leader.rstrip("/")
    topic = args.topic

    # --- PERUBAHAN 3: Tambahkan TCPConnector ---
    conn_limit = max(100, args.concurrency * 2)
    conn = TCPConnector(limit_per_host=conn_limit)

    async with ClientSession(connector=conn) as sess:
        async def prod_worker(i: int):
            payload = {"i": i, "ts": time.time()}
            ok, lat, _ = await timed_request(
                sess, "POST", f"{base}/queue/produce",
                json_body={"topic": topic, "payload": payload},
                timeout=args.timeout,
            )
            stats.rec("queue.produce", ok, lat)
        await run_pool(args.iterations, args.concurrency, prod_worker)

        acked = 0
        sem = asyncio.Semaphore(args.concurrency)
        async def cons_ack_one():
            nonlocal acked
            async with sem:
                ok, lat, data = await timed_request(
                    sess, "GET",
                    f"{base}/queue/consume?topic={topic}&max=1&visibility_ttl={args.visibility_ttl}",
                    timeout=args.timeout,
                )
                stats.rec("queue.consume", ok, lat)
                if ok:
                    msgs = data.get("messages") or []
                    if msgs:
                        mid = msgs[0].get("msg_id") or msgs[0].get("id")
                        if mid:
                            ok2, lat2, _ = await timed_request(
                                sess, "POST", f"{base}/queue/ack",
                                json_body={"topic": topic, "msg_id": mid},
                                timeout=args.timeout,
                            )
                            stats.rec("queue.ack", ok2, lat2)
                            if ok2:
                                acked += 1
        while acked < args.iterations:
            batch = min(args.iterations - acked, args.concurrency)
            await asyncio.gather(*(cons_ack_one() for _ in range(batch)))

async def scenario_requeue(args, stats: BenchStats):
    base = args.leader.rstrip("/")
    topic = args.topic

    # --- PERUBAHAN 4: Tambahkan TCPConnector ---
    conn_limit = max(100, args.concurrency * 2)
    conn = TCPConnector(limit_per_host=conn_limit)
    
    async with ClientSession(connector=conn) as sess:
        async def prod_worker(i: int):
            payload = {"i": i, "ts": time.time()}
            ok, lat, _ = await timed_request(
                sess, "POST", f"{base}/queue/produce",
                json_body={"topic": topic, "payload": payload},
                timeout=args.timeout,
            )
            stats.rec("queue.produce", ok, lat)
        await run_pool(args.iterations, args.concurrency, prod_worker)

        consumed = 0
        async def cons_one():
            nonlocal consumed
            ok, lat, _ = await timed_request(
                sess, "GET",
                f"{base}/queue/consume?topic={topic}&max=1&visibility_ttl={args.visibility_ttl}",
                timeout=args.timeout,
            )
            stats.rec("queue.consume", ok, lat)
            if ok:
                consumed += 1
        while consumed < args.iterations:
            batch = min(args.iterations - consumed, args.concurrency)
            await asyncio.gather(*(cons_one() for _ in range(batch)))

        await asyncio.sleep(args.visibility_ttl + 1)

        acked = 0
        async def cons_ack_one():
            nonlocal acked
            ok, lat, data = await timed_request(
                sess, "GET",
                f"{base}/queue/consume?topic={topic}&max=1&visibility_ttl={args.visibility_ttl}",
                timeout=args.timeout,
            )
            stats.rec("queue.consume", ok, lat)
            if ok:
                msgs = data.get("messages") or []
                if msgs:
                    mid = msgs[0].get("msg_id") or msgs[0].get("id")
                    if mid:
                        ok2, lat2, _ = await timed_request(
                            sess, "POST", f"{base}/queue/ack",
                            json_body={"topic": topic, "msg_id": mid},
                            timeout=args.timeout,
                        )
                        stats.rec("queue.ack", ok2, lat2)
                        if ok2:
                            acked += 1
        while acked < args.iterations:
            batch = min(args.iterations - acked, args.concurrency)
            await asyncio.gather(*(cons_ack_one() for _ in range(batch)))

# ===================== CLI & printing =====================
# ... (Tidak ada perubahan di bagian ini) ...
def print_summary(args, stats: BenchStats, target_desc: str):
    line()
    print("Benchmark Results:")
    line()
    print(f"Total Requests       : {stats.total_req:>6}")
    print(f"Successful Requests  : {stats.total_ok:>6}")
    print(f"Failed Requests      : {stats.total_fail:>6}")
    print(f"Average Latency      : {stats.avg_latency:8.2f} ms")
    print(f"Total Time Taken     : {stats.duration:8.2f} s")
    print(f"Throughput           : {stats.throughput:8.2f} req/sec")
    line()
    if stats.ops:
        print("Per-operation breakdown:")
        print(f"{'Op':<18} {'Req':>8} {'OK':>8} {'Avg(ms)':>10}")
        for op, s in stats.ops.items():
            print(f"{op:<18} {s.n:>8} {s.ok:>8} {s.avg_ms():>10.2f}")
        line()
    print("[INFO] Scenario       :", args.scenario)
    print("[INFO] Target         :", target_desc)
    print("[INFO] Concurrency    :", args.concurrency)
    print("[INFO] Iterations     :", args.iterations)
    if args.scenario != "lock":
        print("[INFO] Topic          :", args.topic)
        print("[INFO] Visibility TTL :", args.visibility_ttl, "s")
    line("=")

async def main_async(args):
    line()
    print("[INFO] Starting benchmark...")
    if args.scenario == "lock":
        desc = f"{args.leader.rstrip('/')}/lock/*"
    elif args.scenario == "roundtrip":
        desc = f"{args.leader.rstrip('/')}/queue{{produce,consume,ack}}"
    else:
        desc = f"{args.leader.rstrip('/')}/queue (requeue TTL={args.visibility_ttl}s)"
    approx = {"lock": args.iterations * 2, "roundtrip": args.iterations * 3, "requeue": args.iterations * 3}[args.scenario]
    print("[INFO] Target URL   :", desc)
    print("[INFO] Total Requests (approx) :", approx)
    print("[INFO] Concurrency  :", args.concurrency)
    line()

    stats = BenchStats()
    stats.t_start = time.perf_counter()

    if args.scenario == "lock":
        await scenario_lock(args, stats)
    elif args.scenario == "roundtrip":
        await scenario_roundtrip(args, stats)
    elif args.scenario == "requeue":
        await scenario_requeue(args, stats)
    else:
        raise SystemExit(f"Unknown scenario: {args.scenario}")

    stats.t_end = time.perf_counter()
    print_summary(args, stats, desc)

def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="benchmarks.load_test_scenario",
        description="Stdout-only benchmark with robust leader-follow for /lock/*."
    )
    p.add_argument("--scenario", choices=["lock", "roundtrip", "requeue"], required=True)
    p.add_argument("--leader", type=str, default="http://localhost:8081",
                    help="Initial base URL (juga dipakai untuk printing).")
    p.add_argument("--nodes", type=str, nargs="?", const="", default="http://localhost:8080,http://localhost:8081,http://localhost:8082",
                    help="Comma-separated nodes; kosong -> auto-discover 8080..8085.")
    p.add_argument("--iterations", type=int, default=1000)
    p.add_argument("--concurrency", type=int, default=50) # Naikkan default concurrency
    p.add_argument("--topic", type=str, default="bench")
    p.add_argument("--visibility-ttl", type=int, default=5)
    p.add_argument("--timeout", type=float, default=5.0)
    p.add_argument("--resources", type=int, default=256, help="Jumlah resource unik (kurangi kontensi).")
    p.add_argument("--grant-wait-ms", type=int, default=500, help="Tunggu grant setelah acquire (ms). 0 untuk skip.")
    p.add_argument("--verbose", action="store_true", help="Cetak sampel error status/body.")
    return p

def main():
    args = build_parser().parse_args()
    asyncio.run(main_async(args))

if __name__ == "__main__":
    main()
