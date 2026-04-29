import asyncio
import sys

import pytest


@pytest.fixture(autouse=True)
def _isolate_idempotency_state(tmp_path, monkeypatch):
    """Redirect the idempotency state file to tmp_path for every test.

    The processor persists to ``~/.moonraker-owl/idempotency.json`` in
    production (audit A-08); tests must never touch the operator's
    real state file.
    """
    from moonraker_owl import constants as _constants
    from moonraker_owl.commands import processor as _processor

    fake = tmp_path / "idempotency.json"
    monkeypatch.setattr(_constants, "DEFAULT_IDEMPOTENCY_PATH", fake, raising=True)
    monkeypatch.setattr(_processor, "DEFAULT_IDEMPOTENCY_PATH", fake, raising=True)
    yield fake


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
