from __future__ import annotations
import asyncio
import time
from typing import Any, Awaitable, Callable, Dict, List, Optional, Tuple, Union
from aiohttp import web, ClientSession, ClientError

Json = Dict[str, Any]
Handler = Union[Callable[..., Any], Callable[..., Awaitable[Any]]]


class RpcError(RuntimeError):
    pass


def _build_url(peer: str, path: str) -> str:
    # peer: "node2:8081" atau "127.0.0.1:8080"
    if not path.startswith("/"):
        path = "/" + path
    return f"http://{peer}{path}"


class RpcClient:
    """
    RPC client sederhana di atas HTTP JSON.
    - call(): generic HTTP call
    - rpc(): kirim payload {"method": "...", "params": {...}} ke /rpc
    - broadcast(): panggil ke banyak peer paralel, bisa set quorum
    """
    def __init__(
        self,
        session: Optional[ClientSession] = None,
        default_timeout: float = 2.5,
        retries: int = 2,
        backoff: float = 0.5,
        concurrency: int = 32,
    ):
        self._own_session = session is None
        self._session = session or ClientSession()
        self._timeout = default_timeout
        self._retries = retries
        self._backoff = backoff
        self._sem = asyncio.Semaphore(concurrency)

    async def close(self):
        if self._own_session:
            await self._session.close()

    async def call(
        self, peer: str, path: str, json: Optional[Json] = None, method: str = "POST", timeout: Optional=float
    ) -> Any:
        url = _build_url(peer, path)
        last_exc: Optional[Exception] = None
        for attempt in range(self._retries + 1):
            try:
                t = timeout or self._timeout
                async with self._sem:
                    if method.upper() == "GET":
                        async with self._session.get(url, timeout=t) as resp:
                            resp.raise_for_status()
                            return await resp.json()
                    else:
                        async with self._session.request(method.upper(), url, json=json, timeout=t) as resp:
                            resp.raise_for_status()
                            return await resp.json()
            except (ClientError, asyncio.TimeoutError) as e:
                last_exc = e
                if attempt < self._retries:
                    await asyncio.sleep(self._backoff * (attempt + 1))
                else:
                    break
        raise RpcError(f"RPC call failed to {url}: {last_exc}")

    async def rpc(self, peer: str, method: str, params: Optional[Json] = None, timeout: Optional[float] = None) -> Any:
        payload = {"method": method, "params": params or {}, "ts": time.time()}
        data = await self.call(peer, "/rpc", json=payload, method="POST", timeout=timeout)
        if not data.get("ok", False):
            raise RpcError(f"RPC error from {peer}: {data!r}")
        return data.get("result")

    async def broadcast(
        self,
        peers: List[str],
        method: str,
        params: Optional[Json] = None,
        timeout: Optional[float] = None,
        quorum: Optional[int] = None,
    ) -> Dict[str, Tuple[bool, Any]]:
        """
        Kirim RPC yang sama ke banyak peer.
        Return dict: peer -> (success, result_or_exc)
        Jika quorum diset, fungsi tetap menunggu semua task selesai (biar tahu siapa yang gagal),
        tapi kamu bisa evaluasi quorum di hasilnya.
        """
        results: Dict[str, Tuple[bool, Any]] = {}

        async def _one(p: str):
            try:
                res = await self.rpc(p, method, params=params, timeout=timeout)
                results[p] = (True, res)
            except Exception as e:
                results[p] = (False, e)

        tasks = [asyncio.create_task(_one(p)) for p in peers]
        await asyncio.gather(*tasks)

        # (opsional) cek quorum di pemanggil
        return results


class JsonRpcRegistry:
    """
    Registri handler untuk endpoint /rpc.
    Handler menerima dict params, boleh sync/async. Return-nya otomatis di-JSON-kan.
    """
    def __init__(self):
        self._handlers: Dict[str, Handler] = {}

    def register(self, method: str, func: Handler):
        if not callable(func):
            raise TypeError("handler must be callable")
        self._handlers[method] = func

    async def handle(self, request: web.Request) -> web.Response:
        try:
            data = await request.json()
        except Exception:
            return web.json_response({"ok": False, "error": "invalid_json"}, status=400)

        method = data.get("method")
        params = data.get("params", {}) or {}
        if not isinstance(params, dict):
            return web.json_response({"ok": False, "error": "params_must_be_object"}, status=400)

        fn = self._handlers.get(method)
        if not fn:
            return web.json_response({"ok": False, "error": f"unknown_method:{method}"}, status=404)

        try:
            if asyncio.iscoroutinefunction(fn):
                result = await fn(**params)
            else:
                result = fn(**params)
            return web.json_response({"ok": True, "result": result})
        except TypeError as e:
            # kemungkinan mismatch argumen
            return web.json_response({"ok": False, "error": f"bad_params:{e}"}, status=400)
        except Exception as e:
            return web.json_response({"ok": False, "error": f"exception:{e.__class__.__name__}:{e}"}, status=500)
