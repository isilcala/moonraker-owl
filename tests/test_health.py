import aiohttp
import pytest

from moonraker_owl.health import HealthReporter, HealthServer


@pytest.mark.asyncio
async def test_health_reporter_snapshot():
    reporter = HealthReporter()

    await reporter.update("mqtt", True)
    await reporter.update("sensors", False, "stopped")

    snapshot = await reporter.snapshot()

    assert snapshot["status"] == "degraded"
    component_list = snapshot.get("components", [])
    assert isinstance(component_list, list)
    components = {item["name"]: item for item in component_list}
    assert components["mqtt"]["healthy"] is True
    assert components["sensors"]["healthy"] is False
    assert components["sensors"]["detail"] == "stopped"


@pytest.mark.asyncio
async def test_health_reporter_agent_state_affects_status():
    reporter = HealthReporter()

    await reporter.update("mqtt", True)
    await reporter.set_agent_state("awaiting_mqtt", healthy=False)

    snapshot = await reporter.snapshot()

    assert snapshot["status"] == "degraded"
    agent = snapshot.get("agentState")
    assert agent is not None
    assert agent["state"] == "awaiting_mqtt"
    assert agent["healthy"] is False


@pytest.mark.asyncio
async def test_health_server_serves_snapshot(unused_tcp_port):
    reporter = HealthReporter()
    await reporter.update("mqtt", True)

    host = "127.0.0.1"
    port = unused_tcp_port
    server = HealthServer(reporter, host, port)
    await server.start()

    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(f"http://{host}:{port}/healthz") as response:
                payload = await response.json()
                assert response.status == 200
                assert payload["status"] == "ok"
    finally:
        await server.stop()
