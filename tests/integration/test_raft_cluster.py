import asyncio
import socket
import time
import pytest

from src.consensus.raft import RaftNode
from src.nodes.lock_manager import LockTable, LockMode

def _free_port() -> int:
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]

@pytest.mark.asyncio
async def test_election_and_lock_propose():
    # --- shared state machine untuk test ---
    lock_table = LockTable()
    def apply_command(cmd: dict):
        t = cmd.get("type")
        if t == "LOCK_ACQUIRE":
            lock_table.acquire(cmd["resource"], cmd["owner"], cmd["mode"])
        elif t == "LOCK_RELEASE":
            lock_table.release(cmd["resource"], cmd["owner"])

    # --- siapkan 3 port & peers (127.0.0.1) ---
    p1, p2, p3 = _free_port(), _free_port(), _free_port()
    peers1 = [f"127.0.0.1:{p2}", f"127.0.0.1:{p3}"]
    peers2 = [f"127.0.0.1:{p1}", f"127.0.0.1:{p3}"]
    peers3 = [f"127.0.0.1:{p1}", f"127.0.0.1:{p2}"]

    n1 = RaftNode("n1", p1, peers1, apply_fn=apply_command)
    n2 = RaftNode("n2", p2, peers2, apply_fn=apply_command)
    n3 = RaftNode("n3", p3, peers3, apply_fn=apply_command)

    try:
        await asyncio.gather(n1.start(), n2.start(), n3.start())

        # tunggu election (maks 3 detik)
        t0 = time.time()
        leader = None
        while time.time() - t0 < 3.0:
            for n in (n1, n2, n3):
                if n.state == "leader":
                    leader = n
                    break
            if leader:
                break
            await asyncio.sleep(0.05)

        assert leader is not None, "Tidak ada leader terpilih"

        # propose perintah lock via leader
        ok = await leader.propose({
            "type": "LOCK_ACQUIRE",
            "resource": "res-int",
            "owner": "client-A",
            "mode": LockMode.EXCLUSIVE,
        })
        assert ok, "Propose gagal (tidak commit ke mayoritas)"

        # beri waktu applier jalan
        await asyncio.sleep(0.1)

        # verifikasi state machine berubah
        assert lock_table.modes.get("res-int") == LockMode.EXCLUSIVE
        assert "client-A" in lock_table.holders["res-int"]

    finally:
        await asyncio.gather(n1.stop(), n2.stop(), n3.stop())

@pytest.mark.asyncio
async def test_health_endpoints():
    # single node akan auto-elect jadi leader (tanpa peers)
    p = _free_port()
    applied = []
    def apply_command(cmd: dict): applied.append(cmd)

    n = RaftNode("single", p, [], apply_fn=apply_command)
    try:
        await n.start()
        # tunggu jadi leader
        t0 = time.time()
        while time.time() - t0 < 2.0 and n.state != "leader":
            await asyncio.sleep(0.05)
        assert n.state == "leader"

        # cek handler /raft/health (langsung via client aiohttp)
        from aiohttp import ClientSession
        async with ClientSession() as s:
            async with s.get(f"http://127.0.0.1:{p}/raft/health") as resp:
                data = await resp.json()
                assert data["state"] in ("leader", "follower")
                assert "term" in data
    finally:
        await n.stop()
