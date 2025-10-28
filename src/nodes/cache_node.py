from __future__ import annotations
from collections import OrderedDict
from typing import Dict, Any, Optional, Set

class MESI: M='M'; E='E'; S='S'; I='I'

class LRU:
    def __init__(self, cap=1000): self.cap=cap; self.od=OrderedDict()
    def get(self,k):
        if k in self.od: v=self.od.pop(k); self.od[k]=v; return v
        return None
    def put(self,k,v):
        if k in self.od: self.od.pop(k)
        elif len(self.od) >= self.cap: self.od.popitem(last=False)
        self.od[k]=v

class Directory:
    def __init__(self):
        self.state: Dict[str, str] = {}        # key -> MESI
        self.holders: Dict[str, Set[str]] = {} # key -> node_ids

class CacheNode:
    def __init__(self, node_id: str, directory: Directory, max_items=10000):
        self.id = node_id; self.dir = directory; self.cache = LRU(max_items)

    def get(self, key: str) -> Optional[Any]:
        return self.cache.get(key)  # demo: assume already valid

    def put(self, key: str, value: Any):
        # Write: invalidate others lalu ambil M
        self._invalidate_others(key)
        self.dir.state[key] = MESI.M
        self.dir.holders[key] = {self.id}
        self.cache.put(key, value)

    def _invalidate_others(self, key: str):
        for n in list(self.dir.holders.get(key, set())):
            if n != self.id:
                # real-world: kirim RPC invalidate; demo: drop saja
                pass
        self.dir.holders[key] = {self.id}
        self.dir.state[key] = MESI.I
