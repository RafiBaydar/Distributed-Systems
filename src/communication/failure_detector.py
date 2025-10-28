from __future__ import annotations
import asyncio
import time
from typing import Callable, Dict, List, Optional, Tuple
from aiohttp import ClientSession, ClientError

OnChange = Callable[[str, bool, float], None]  # (peer, is_up, rtt_sec)

class FailureDetector:
    """
    Heartbeat checker sederhana:
    - GET /health ke setiap peer secara periodik
    - simpan last_ok, RTT, dan status up/down
    - panggil callback on_change ketika status berubah
    """
    def __init__(
        self,
        peers: List[str],
        path: str = "/health",
        interval: float = 1.0,
        timeout: float = 0.7,
        failure_window: float = 3.0,
    ):
        self.peers = peers
        self.path = path
        self.interval = interval
        self.timeout = timeout
        self.failure_window = failure_window

        self._session: Optional[ClientSession] = None
        self._task: Optional[asyncio.Task] = None
        self._stop = asyncio.Event()

        self._last_ok: Dict[str, float] = {p: 0.0 for p in peers}
        self._rtt: Dict[str, float] = {p: float("inf") for p in peers}
        self._is_up: Dict[str, bool] = {p: False for p in peers}
        self._listeners: List[OnChange] = []

    def subscribe(self, cb: OnChange):
        self._listeners.append(cb)

    def _emit(self, peer: str, up: bool):
        rtt = self._rtt.get(peer, float("inf"))
        for cb in list(self._listeners):
            try:
                cb(peer, up, rtt)
            except Exception:
                pass

    async def start(self):
        self._session = ClientSession()
        self._stop.clear()
        self._task = asyncio.create_task(self._run())

    async def stop(self):
        self._stop.set()
        if self._task:
            await self._task
        if self._session:
            await self._session.close()

    async def _probe(self, peer: str):
        assert self._session is not None
        url = f"http://{peer}{self.path}"
        t0 = time.perf_counter()
        try:
            async with self._session.get(url, timeout=self.timeout) as resp:
                resp.raise_for_status()
                _ = await resp.json()
                rtt = time.perf_counter() - t0
                self._rtt[peer] = rtt
                self._last_ok[peer] = time.time()
                if not self._is_up[peer]:
                    self._is_up[peer] = True
                    self._emit(peer, True)
                return
        except (ClientError, asyncio.TimeoutError):
            # tidak langsung down — lihat window di _evaluate()
            pass

        # evaluate after failed attempt
        now = time.time()
        if (now - self._last_ok[peer]) > self.failure_window:
            if self._is_up[peer]:
                self._is_up[peer] = False
                self._emit(peer, False)

    async def _run(self):
        while not self._stop.is_set():
            await asyncio.gather(*[self._probe(p) for p in self.peers])
            await asyncio.wait([self._stop.wait()], timeout=self.interval)

    # --- util untuk akses status saat ini ---
    def snapshot(self) -> Dict[str, Dict[str, float]]:
        now = time.time()
        return {
            p: {
                "up": float(self._is_up[p]),
                "last_ok_age_sec": max(0.0, now - self._last_ok[p]),
                "rtt_sec": self._rtt[p],
            }
            for p in self.peers
        }
