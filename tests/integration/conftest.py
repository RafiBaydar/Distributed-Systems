import asyncio
import sys
import pytest

@pytest.fixture(scope="session")
def event_loop():
    # Pastikan selector loop di Windows supaya aiohttp/asyncio stabil
    if sys.platform.startswith("win"):
        try:
            asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())
        except Exception:
            pass
    loop = asyncio.new_event_loop()
    yield loop
    loop.close()
