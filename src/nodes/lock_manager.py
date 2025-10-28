from __future__ import annotations
from typing import Dict, Set, Optional, List, Deque, Tuple
from collections import defaultdict, deque

class LockMode:
    SHARED = 's'
    EXCLUSIVE = 'x'

class LockTable:
    def __init__(self):
        self.holders: Dict[str, Set[str]] = defaultdict(set)   # res -> owners
        self.modes: Dict[str, str] = {}                        # res -> mode
        self.waitq: Dict[str, Deque[Tuple[str, str]]] = defaultdict(deque)  # res -> [(owner, mode)]
        self.waits_for: Dict[str, Set[str]] = defaultdict(set)  # owner -> set(owners)

    def acquire(self, resource: str, owner: str, mode: str) -> bool:
        cur_mode = self.modes.get(resource)
        cur_holders = self.holders[resource]
        # available
        if not cur_holders:
            self.modes[resource] = mode
            cur_holders.add(owner)
            return True
        # shared-compatible
        if mode == LockMode.SHARED and cur_mode == LockMode.SHARED:
            cur_holders.add(owner)
            return True
        # conflict -> enqueue + waits-for
        self.waitq[resource].append((owner, mode))
        for h in cur_holders:
            self.waits_for[owner].add(h)
        return False

    def release(self, resource: str, owner: str) -> None:
        cur_holders = self.holders[resource]
        if owner in cur_holders:
            cur_holders.remove(owner)
        if not cur_holders:
            # grant next
            while self.waitq[resource]:
                next_owner, next_mode = self.waitq[resource][0]
                if next_mode == LockMode.SHARED:
                    self.modes[resource] = LockMode.SHARED
                    # grant semua request shared yang berurutan di depan antrian
                    while self.waitq[resource] and self.waitq[resource][0][1] == LockMode.SHARED:
                        o, _ = self.waitq[resource].popleft()
                        self.holders[resource].add(o)
                        self._clear_waits_for(o)
                    break
                else:
                    # exclusive: hanya head of queue
                    self.waitq[resource].popleft()
                    self.modes[resource] = LockMode.EXCLUSIVE
                    self.holders[resource].add(next_owner)
                    self._clear_waits_for(next_owner)
                    break

    def detect_deadlock(self) -> Optional[List[str]]:
        color: Dict[str, str] = {}
        stack: List[str] = []

        def dfs(u: str) -> Optional[List[str]]:
            color[u] = 'gray'
            stack.append(u)
            for v in self.waits_for.get(u, set()):
                if color.get(v) == 'gray':
                    # cycle ditemukan
                    i = stack.index(v)
                    return stack[i:] + [v]
                if color.get(v) is None:
                    cyc = dfs(v)
                    if cyc:
                        return cyc
            stack.pop()
            color[u] = 'black'
            return None

        for n in list(self.waits_for.keys()):
            if color.get(n) is None:
                cyc = dfs(n)
                if cyc:
                    return cyc
        return None

    def _clear_waits_for(self, owner: str):
        self.waits_for.pop(owner, None)
        for deps in self.waits_for.values():
            deps.discard(owner)
