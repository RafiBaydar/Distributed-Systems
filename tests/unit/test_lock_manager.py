from src.nodes.lock_manager import LockTable, LockMode

def test_shared_compatible():
    lt = LockTable()
    assert lt.acquire('r','a',LockMode.SHARED)
    assert lt.acquire('r','b',LockMode.SHARED)

def test_exclusive_blocks():
    lt = LockTable()
    assert lt.acquire('r','a',LockMode.EXCLUSIVE)
    assert not lt.acquire('r','b',LockMode.SHARED)
