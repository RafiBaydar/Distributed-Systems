from __future__ import annotations
import asyncio, time, random
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Callable
from aiohttp import web, ClientSession

@dataclass
class LogEntry:
    term: int
    command: Dict[str, Any]

class RaftNode:
    def __init__(self, node_id: str, port: int, peers: List[str],
                 apply_fn: Callable[[Dict[str, Any]], None], 
                 election_min_ms=900, election_max_ms=1500, heartbeat_ms=200):
        self.id = node_id
        self.port = port
        self.peers = [p for p in peers if p]  # "host:port"
        self.apply_fn = apply_fn
        self.term = 0
        self.voted_for: Optional[str] = None
        self.log: List[LogEntry] = []
        self.commit_index = -1
        self.last_applied = -1
        self.state = "follower"  # follower|candidate|leader
        self.last_heartbeat = time.time()
        self.election_min_ms = election_min_ms
        self.election_max_ms = election_max_ms
        self.heartbeat_ms = heartbeat_ms

        # --- FIX (Bug 2): Gunakan self._app (underscore) ---
        self._app = web.Application()
        self._app.add_routes([
            web.get('/raft/health', self._health),
            web.post('/raft/request_vote', self._request_vote),
            web.post('/raft/append_entries', self._append_entries),
        ])
        self._runner: Optional[web.AppRunner] = None
        self._site: Optional[web.TCPSite] = None
        self._session: Optional[ClientSession] = None
        
        # --- FIX (Bug 3): Tambahkan Lock untuk Propose ---
        self._propose_lock = asyncio.Lock()

    async def start(self):
        self._runner = web.AppRunner(self._app) # Pastikan ini juga _app
        await self._runner.setup()
        self._site = web.TCPSite(self._runner, '0.0.0.0', self.port)
        await self._site.start()
        self._session = ClientSession()
        asyncio.create_task(self._ticker())
        asyncio.create_task(self._applier())

    async def stop(self):
        if self._session: await self._session.close()
        if self._runner: await self._runner.cleanup()

    async def propose(self, command: Dict[str, Any]) -> bool:
        # --- FIX (Bug 3): Lindungi propose() dari race condition ---
        async with self._propose_lock:
            if self.state != 'leader':
                raise web.HTTPConflict(text='Not leader')
            
            self.log.append(LogEntry(self.term, command))
            index = len(self.log) - 1
            acks = 1
            
            tasks = []
            for peer in self.peers:
                # Kirim append ke semua follower secara paralel
                tasks.append(asyncio.create_task(self._send_append(peer)))
            
            results = await asyncio.gather(*tasks)
            for ok in results:
                if ok:
                    acks += 1

            if acks >= (len(self.peers) + 1) // 2 + 1:
                self.commit_index = index
                return True
            
            return False

    # ---- background ----
    async def _ticker(self):
        while True:
            await asyncio.sleep(0.05)
            if self.state in ('follower', 'candidate'):
                if (time.time() - self.last_heartbeat) * 1000 > random.randint(self.election_min_ms, self.election_max_ms):
                    await self._start_election()
            elif self.state == 'leader':
                # Kirim heartbeat secara paralel
                tasks = []
                for p in self.peers:
                    tasks.append(asyncio.create_task(self._send_append(p, heartbeat=True)))
                await asyncio.gather(*tasks)
                await asyncio.sleep(self.heartbeat_ms / 1000)

    async def _applier(self):
        while True:
            await asyncio.sleep(0.01)
            while self.commit_index > self.last_applied:
                self.last_applied += 1
                entry = self.log[self.last_applied]
                try:
                    # --- FIX (Bug 1): Tambahkan 'await' ---
                    await self.apply_fn(entry.command)
                except Exception as e:
                    print(f"[APPLY ERROR] Failed to apply log {self.last_applied}: {e}")

    async def _start_election(self):
        self.state = 'candidate'
        self.term += 1
        self.voted_for = self.id
        votes = 1
        self.last_heartbeat = time.time()
        last_index = len(self.log) - 1
        last_term = self.log[last_index].term if last_index >= 0 else 0
        
        tasks = []
        for peer in self.peers:
            tasks.append(asyncio.create_task(self._send_vote_request(peer, last_index, last_term)))
            
        results = await asyncio.gather(*tasks)
        for ok in results:
            if ok:
                votes += 1

        if votes >= (len(self.peers) + 1) // 2 + 1:
            self.state = 'leader'
        else:
            self.state = 'follower'

    async def _send_vote_request(self, peer: str, last_index: int, last_term: int) -> bool:
        try:
            async with self._session.post(f'http://{peer}/raft/request_vote', json={
                "term": self.term, "candidate_id": self.id,
                "last_log_index": last_index, "last_log_term": last_term,
            }, timeout=0.5) as resp: # Tambahkan timeout pendek
                if resp.status == 200 and (await resp.json()).get('vote_granted'):
                    return True
        except Exception:
            pass
        return False

    async def _send_append(self, peer: str, heartbeat=False) -> bool:
        # Ini masih implementasi sederhana, idealnya butuh nextIndex per follower
        prev_idx = len(self.log) - (1 if heartbeat else 2)
        prev_term = self.log[prev_idx].term if prev_idx >= 0 else 0
        entries = [] if heartbeat else ([self.log[-1].__dict__] if self.log else [])
        
        try:
            async with self._session.post(f'http://{peer}/raft/append_entries', json={
                "term": self.term, "leader_id": self.id,
                "prev_log_index": prev_idx, "prev_log_term": prev_term,
                "entries": entries, "leader_commit": self.commit_index,
            }, timeout=0.5) as resp: # Tambahkan timeout pendek
                if resp.status == 200:
                    # self.last_heartbeat = time.time() # Sebaiknya di set di ticker, bukan di sini
                    return (await resp.json()).get('success', False)
                else:
                    # Jika follower menolak (term lebih tinggi), update term kita
                    if resp.status == 400: # Asumsi 400 jika term < follower.term
                        data = await resp.json()
                        if data.get('term', 0) > self.term:
                            self.term = data['term']
                            self.state = 'follower'
                            self.voted_for = None
                    return False
        except Exception:
            return False
        return False

    # ---- HTTP handlers ----
    async def _health(self, _req): return web.json_response({"node": self.id, "state": self.state, "term": self.term})

    async def _request_vote(self, req: web.Request):
        d = await req.json(); term = d['term']
        if term > self.term:
            self.term = term; self.voted_for = None; self.state = 'follower'
        
        # Log matching check
        last_index = len(self.log) - 1
        last_term = self.log[last_index].term if last_index >= 0 else 0
        
        log_ok = (d['last_log_term'] > last_term) or \
                 (d['last_log_term'] == last_term and d['last_log_index'] >= last_index)

        vote = False
        if (self.voted_for in (None, d['candidate_id'])) and term == self.term and log_ok:
            self.voted_for = d['candidate_id']; vote = True; self.last_heartbeat = time.time()
        
        return web.json_response({"term": self.term, "vote_granted": vote})

    async def _append_entries(self, req: web.Request):
        d = await req.json(); term = d['term']
        if term < self.term: 
            return web.json_response({"term": self.term, "success": False}, status=400) # Kirim term kita
        
        # Jika kita terima append, kita pasti follower (atau leader/candidate yg outdated)
        self.state = 'follower'; self.last_heartbeat = time.time()
        
        # Update term jika term leader lebih baru
        if term > self.term:
            self.term = term
            self.voted_for = None

        prev_idx, prev_term = d['prev_log_index'], d['prev_log_term']
        
        if prev_idx >= 0 and (prev_idx >= len(self.log) or self.log[prev_idx].term != prev_term):
            # Follower tertinggal atau log-nya beda
            return web.json_response({"term": self.term, "success": False})
        
        # Hapus log yang konflik (jika ada) dan tambahkan yang baru
        if len(d['entries']) > 0:
             # Asumsi sederhana: hanya 1 entry per request
             entry_idx = prev_idx + 1
             if entry_idx < len(self.log):
                 # Jika log di index ini beda, hapus sisa log
                 if self.log[entry_idx].term != d['entries'][0]['term']:
                     self.log = self.log[:entry_idx]
             
             for e in d['entries']:
                 if entry_idx < len(self.log):
                     if self.log[entry_idx].term != e['term']:
                         self.log = self.log[:entry_idx]
                         self.log.append(LogEntry(**e))
                 else:
                     self.log.append(LogEntry(**e))
                 entry_idx += 1


        if d['leader_commit'] > self.commit_index:
            self.commit_index = min(d['leader_commit'], len(self.log) - 1)
        
        return web.json_response({"term": self.term, "success": True})

    # 'app' adalah property read-only ke '_app'
    @property
    def app(self):  # akses dari base_node.py
        return self._app

