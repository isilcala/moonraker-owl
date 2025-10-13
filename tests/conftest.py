import asyncio
import sys

import pytest


@pytest.fixture
def loop_factory():
    """Provide event loops for aiohttp pytest integration."""
    loops: list[asyncio.AbstractEventLoop] = []

    def factory() -> asyncio.AbstractEventLoop:
        if sys.platform.startswith("win"):
            loop = asyncio.SelectorEventLoop()
        else:
            loop = asyncio.new_event_loop()

        asyncio.set_event_loop(loop)
        loops.append(loop)
        return loop

    yield factory

    for loop in loops:
        loop.close()

    asyncio.set_event_loop(None)
