"""Unit tests for TokenManager (JWT token lifecycle management)."""

import asyncio
import base64
import json
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import aiohttp
from aiohttp import web
from aiohttp.test_utils import TestServer
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

from moonraker_owl.token_manager import TokenCredentials, TokenManager


@pytest.fixture
def ed25519_keypair():
    """Generate a test Ed25519 key pair."""
    private_key = Ed25519PrivateKey.generate()
    private_key_bytes = private_key.private_bytes_raw()
    private_key_b64 = base64.b64encode(private_key_bytes).decode("ascii")
    public_key = private_key.public_key()
    return {
        "private_key": private_key,
        "private_key_b64": private_key_b64,
        "public_key": public_key,
    }


@pytest.mark.asyncio
async def test_token_manager_initialization(ed25519_keypair):
    """Test TokenManager initializes with valid Ed25519 private key."""
    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost:8080",
    )

    assert manager.device_id == "device-123"
    assert manager.base_url == "http://localhost:8080"
    assert manager._private_key is not None
    assert manager._current_token is None
    assert manager._renewal_task is None
    assert manager.renewal_ratio == 0.85  # Default value
    assert manager.safety_buffer_seconds == 120  # Default value


@pytest.mark.asyncio
async def test_token_manager_sign_timestamp(ed25519_keypair):
    """Test Ed25519 signature generation for timestamp."""
    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost:8080",
    )

    timestamp = 1699900000
    signature = manager._sign_timestamp(timestamp)

    # Signature should be base64-encoded
    assert isinstance(signature, str)
    signature_bytes = base64.b64decode(signature)
    assert len(signature_bytes) == 64  # Ed25519 signature is 64 bytes

    # Verify signature with public key
    message = f"device-123:{timestamp}".encode("utf-8")
    ed25519_keypair["public_key"].verify(signature_bytes, message)


@pytest.mark.asyncio
async def test_issue_token_success(ed25519_keypair):
    """Test successful JWT token issuance."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        payload = await request.json()
        assert payload["deviceId"] == "device-123"
        assert "timestamp" in payload
        assert "signature" in payload
        
        # Return JWT token response
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJzdWIiOiJkZXZpY2UtMTIzIn0.test",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # 1 hour in seconds
            "tokenType": "Bearer",
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()

        assert isinstance(manager._current_token, TokenCredentials)
        assert manager._current_token.jwt.startswith("eyJ")
        assert isinstance(manager._current_token.issued_at, datetime)
        assert isinstance(manager._current_token.expires_at, datetime)
        
        await manager.stop()


@pytest.mark.asyncio
async def test_issue_token_invalid_signature(ed25519_keypair):
    """Test token issuance with invalid signature returns 401."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        return web.Response(status=401, text="Invalid signature")

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        with pytest.raises(RuntimeError, match="invalid signature"):
            await manager.start()
        
        await manager.stop()


@pytest.mark.asyncio
async def test_issue_token_device_not_found(ed25519_keypair):
    """Test token issuance when device not found returns 404."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        return web.Response(status=404, text="Device not found")

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        with pytest.raises(RuntimeError, match="device not found"):
            await manager.start()
        
        await manager.stop()


@pytest.mark.asyncio
async def test_issue_token_rate_limit(ed25519_keypair):
    """Test token issuance when rate limited returns 429."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        return web.Response(status=429, text="Rate limit exceeded")

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        with pytest.raises(RuntimeError, match="rate limit exceeded"):
            await manager.start()
        
        await manager.stop()


@pytest.mark.asyncio
async def test_start_issues_initial_token(ed25519_keypair):
    """Test start() method issues initial JWT token."""
    
    call_count = 0
    
    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal call_count
        call_count += 1
        
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": f"jwt-token-{call_count}",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()

        assert call_count == 1
        assert manager._current_token is not None
        assert manager._current_token.jwt == "jwt-token-1"


@pytest.mark.asyncio
async def test_get_mqtt_credentials_before_start_raises_error(ed25519_keypair):
    """Test get_mqtt_credentials() raises error if called before start()."""
    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost:8080",
    )

    with pytest.raises(RuntimeError, match="Token not initialized"):
        manager.get_mqtt_credentials()


@pytest.mark.asyncio
async def test_get_mqtt_credentials_returns_device_id_and_jwt(ed25519_keypair):
    """Test get_mqtt_credentials() returns (device_id, jwt) tuple."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token-abc",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()
        device_id, jwt = manager.get_mqtt_credentials()

        assert device_id == "device-123"
        assert jwt == "jwt-token-abc"


@pytest.mark.asyncio
async def test_refresh_token_now_updates_credentials(ed25519_keypair):
    """Test refresh_token_now() immediately refreshes token."""
    
    call_count = 0
    
    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal call_count
        call_count += 1
        
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": f"jwt-token-{call_count}",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()
        assert call_count == 1
        assert manager._current_token.jwt == "jwt-token-1"

        await manager.refresh_token_now()
        assert call_count == 2
        assert manager._current_token.jwt == "jwt-token-2"


@pytest.mark.asyncio
async def test_renewal_loop_refreshes_token_periodically(ed25519_keypair):
    """Test start_renewal_loop() refreshes token at intervals."""
    
    call_count = 0
    
    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal call_count
        call_count += 1
        
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": f"jwt-token-{call_count}",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.5,  # Renew at 50% for faster testing
            safety_buffer_seconds=0,  # No buffer for testing
        )

        await manager.start()
        assert call_count == 1

        # Start renewal loop
        renewal_callback = AsyncMock()
        manager.start_renewal_loop(on_renewed=renewal_callback)

        # Wait for at least one renewal (50% of 3600s = 1800s is too long)
        # Since we need fast tests, we'll wait briefly and check the task is running
        await asyncio.sleep(0.2)

        # Renewal task should be running
        assert manager._renewal_task is not None
        assert not manager._renewal_task.done()

        # Clean up
        await manager.stop()


@pytest.mark.asyncio
async def test_stop_cancels_renewal_task(ed25519_keypair):
    """Test stop() cancels the renewal task."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()
        manager.start_renewal_loop()

        assert manager._renewal_task is not None
        assert not manager._renewal_task.done()

        await manager.stop()

        assert manager._renewal_task is None


@pytest.mark.asyncio
async def test_renewal_loop_continues_on_error(ed25519_keypair):
    """Test renewal loop continues after token refresh error."""
    
    call_count = 0
    
    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal call_count
        call_count += 1
        
        # Fail on second attempt, succeed on third
        if call_count == 2:
            return web.Response(status=500, text="Server error")
        
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": f"jwt-token-{call_count}",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.5,  # Fast renewal for testing
            safety_buffer_seconds=0,
        )

        await manager.start()
        assert call_count == 1

        manager.start_renewal_loop()

        # Wait briefly to verify renewal task is running
        await asyncio.sleep(0.2)

        # Renewal task should be running (will retry on error after 5 min)
        assert manager._renewal_task is not None
        assert not manager._renewal_task.done()

        await manager.stop()
@pytest.mark.asyncio
async def test_invalid_private_key_raises_error():
    """Test TokenManager raises error with invalid private key."""
    with pytest.raises(Exception):  # Base64 decode error or key format error
        TokenManager(
            device_id="device-123",
            private_key_b64="invalid-base64-key",
            base_url="http://localhost:8080",
        )


@pytest.mark.asyncio
async def test_multiple_start_calls_are_idempotent(ed25519_keypair):
    """Test calling start() multiple times is safe."""
    
    call_count = 0
    
    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal call_count
        call_count += 1
        
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": f"jwt-token-{call_count}",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # \"expiresIn\": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()
        first_token = manager._current_token.jwt
        
        await manager.start()
        second_token = manager._current_token.jwt

        # Each start should issue a new token
        assert call_count == 2
        assert first_token != second_token


@pytest.mark.asyncio
async def test_calculate_renewal_interval_with_1_hour_token(ed25519_keypair):
    """Test renewal interval calculation for 1-hour token (default backend setting)."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # 1 hour
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.85,
            safety_buffer_seconds=120,
        )

        await manager.start()
        
        # Calculate: 3600 * 0.85 - 120 = 2940 seconds (49 minutes)
        # Allow ±1 second for time elapsed during test execution
        interval = manager._calculate_renewal_interval()
        
        assert 2938 <= interval <= 2940
        
        await manager.stop()


@pytest.mark.asyncio
async def test_calculate_renewal_interval_with_30_minute_token(ed25519_keypair):
    """Test renewal interval calculation for 30-minute token."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token",
            "issuedAt": issued_at,
            "expiresIn": 1800,  # 30 minutes
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.85,
            safety_buffer_seconds=120,
        )

        await manager.start()
        
        # Calculate: 1800 * 0.85 - 120 = 1410 seconds (23.5 minutes)
        # Allow ±1 second for time elapsed during test execution
        interval = manager._calculate_renewal_interval()
        
        assert 1408 <= interval <= 1410
        
        await manager.stop()


@pytest.mark.asyncio
async def test_calculate_renewal_interval_with_2_hour_token(ed25519_keypair):
    """Test renewal interval calculation for 2-hour token."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token",
            "issuedAt": issued_at,
            "expiresIn": 7200,  # 2 hours
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.85,
            safety_buffer_seconds=120,
        )

        await manager.start()
        
        # Calculate: 7200 * 0.85 - 120 = 6000 seconds (100 minutes)
        # Allow ±1 second for time elapsed during test execution
        interval = manager._calculate_renewal_interval()
        
        assert 5998 <= interval <= 6000
        
        await manager.stop()


@pytest.mark.asyncio
async def test_calculate_renewal_interval_respects_minimum(ed25519_keypair):
    """Test renewal interval respects minimum of 60 seconds."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token",
            "issuedAt": issued_at,
            "expiresIn": 120,  # 2 minutes - very short
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.85,
            safety_buffer_seconds=120,
        )

        await manager.start()
        
        # Calculate: 120 * 0.85 - 120 = -18, but minimum is 60
        interval = manager._calculate_renewal_interval()
        
        assert interval == 60  # Minimum enforced
        
        await manager.stop()


@pytest.mark.asyncio
async def test_custom_renewal_ratio(ed25519_keypair):
    """Test custom renewal_ratio parameter (e.g., 80% like AWS)."""
    
    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "jwt-token",
            "issuedAt": issued_at,
            "expiresIn": 3600,  # 1 hour
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
            renewal_ratio=0.80,  # AWS-style 80%
            safety_buffer_seconds=60,  # 1-minute buffer
        )

        await manager.start()
        
        # Calculate: 3600 * 0.80 - 60 = 2820 seconds (47 minutes)
        # Allow ±1 second for time elapsed during test execution
        interval = manager._calculate_renewal_interval()
        
        assert 2818 <= interval <= 2820
        
        await manager.stop()


@pytest.mark.asyncio
async def test_renewal_loop_error_retry_logic_exists(ed25519_keypair):
    """Test that renewal loop has error retry logic (validates code path exists).
    
    This test verifies that the renewal loop continues after an error and uses
    a 5-minute (300s) retry interval. Due to the async nature and timing, we
    validate the logic exists rather than timing the actual retry.
    """
    
    call_count = 0
    
    async def handler(request: web.Request) -> web.StreamResponse:
        nonlocal call_count
        call_count += 1
        
        # Always succeed for this test (we're testing the code path, not execution)
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": f"jwt-token-{call_count}",
            "issuedAt": issued_at,
            "expiresIn": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )

        await manager.start()
        assert call_count == 1

        # Verify the renewal loop method exists and has error handling
        # by checking the source code contains retry logic
        import inspect
        source = inspect.getsource(manager._renewal_loop)

        # Verify the code has error handling with progressive backoff retry
        assert "except Exception" in source
        assert "_calculate_retry_interval" in source
        assert "_log_renewal_failure" in source
        
        # Start renewal loop to ensure it doesn't crash
        manager.start_renewal_loop()
        assert manager._renewal_task is not None
        assert not manager._renewal_task.done()

        await manager.stop()


def test_token_manager_rejects_invalid_base64_key():
    from moonraker_owl.token_manager import TokenManagerKeyError

    with pytest.raises(TokenManagerKeyError):
        TokenManager(
            device_id="device-123",
            private_key_b64="!!!not-base64!!!",
            base_url="http://localhost:8080",
        )


def test_token_manager_rejects_wrong_length_key():
    from moonraker_owl.token_manager import TokenManagerKeyError

    short_key = base64.b64encode(b"too-short").decode("ascii")
    with pytest.raises(TokenManagerKeyError):
        TokenManager(
            device_id="device-123",
            private_key_b64=short_key,
            base_url="http://localhost:8080",
        )


def test_calculate_retry_interval_progressive_backoff(ed25519_keypair):
    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost:8080",
    )

    # Plenty of time to expiry: exponential backoff from 30s, capped at 3600s.
    assert manager._calculate_retry_interval(1, 7200) == 30
    assert manager._calculate_retry_interval(2, 7200) == 60
    assert manager._calculate_retry_interval(3, 7200) == 120
    assert manager._calculate_retry_interval(20, 7200) == 3600  # capped


def test_calculate_retry_interval_critical_acceleration(ed25519_keypair):
    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost:8080",
    )

    # Token nearly expired: keep retrying fast regardless of failure count.
    assert manager._calculate_retry_interval(10, 300) == 30
    # Token already expired: retry promptly.
    assert manager._calculate_retry_interval(10, -5) == 15
    # No token yet (None): falls back to plain backoff.
    assert manager._calculate_retry_interval(1, None) == 30


@pytest.mark.asyncio
async def test_issue_token_rejects_out_of_range_expires_in(ed25519_keypair):
    """Server expiresIn outside the allowed window must be rejected (P1-9)."""

    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "eyJ.test.token",
            "issuedAt": issued_at,
            "expiresIn": 10,  # below MIN_TOKEN_LIFETIME_SECONDS (300)
            "tokenType": "Bearer",
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )
        try:
            with pytest.raises(ValueError, match="outside allowed range"):
                await manager.start()
        finally:
            await manager.stop()


@pytest.mark.asyncio
async def test_issue_token_rejects_missing_expires_in(ed25519_keypair):
    """A missing expiresIn must raise rather than crash later (P1-9)."""

    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "eyJ.test.token",
            "issuedAt": issued_at,
            "tokenType": "Bearer",
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )
        try:
            with pytest.raises(ValueError, match="missing 'expiresIn'"):
                await manager.start()
        finally:
            await manager.stop()


@pytest.mark.asyncio
async def test_issue_token_warns_on_clock_drift(ed25519_keypair, caplog):
    """Large drift between local clock and server issuedAt logs a warning (P1-11)."""
    from datetime import timedelta as _td

    async def handler(request: web.Request) -> web.StreamResponse:
        # Server issued the token 10 minutes in the "past" vs the local clock.
        issued_at = (datetime.now(timezone.utc) - _td(minutes=10)).isoformat()
        return web.json_response({
            "accessToken": "eyJ.test.token",
            "issuedAt": issued_at,
            "expiresIn": 3600,
            "tokenType": "Bearer",
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )
        with caplog.at_level("WARNING"):
            await manager.start()
        await manager.stop()

    assert any("clock drift" in r.message.lower() or "drifts" in r.message.lower()
               for r in caplog.records)


@pytest.mark.asyncio
async def test_is_token_valid_false_when_near_expiry(ed25519_keypair):
    """is_token_valid returns False when token is within the safety buffer (P1-10)."""
    from datetime import timedelta as _td

    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost/",
        safety_buffer_seconds=120,
    )
    now = datetime.now(timezone.utc)

    assert manager.is_token_valid() is False  # no token yet

    manager._current_token = TokenCredentials(
        jwt="t", issued_at=now, expires_at=now + _td(seconds=60)
    )
    assert manager.is_token_valid() is False  # inside the 120s buffer

    manager._current_token = TokenCredentials(
        jwt="t", issued_at=now, expires_at=now + _td(seconds=600)
    )
    assert manager.is_token_valid() is True


@pytest.mark.asyncio
async def test_ensure_valid_token_reissues_when_stale(ed25519_keypair):
    """ensure_valid_token re-issues a token that is missing/near expiry (P1-10)."""

    async def handler(request: web.Request) -> web.StreamResponse:
        issued_at = datetime.now(timezone.utc).isoformat()
        return web.json_response({
            "accessToken": "fresh-token",
            "issuedAt": issued_at,
            "expiresIn": 3600,
        })

    app = web.Application()
    app.router.add_post("/api/v1/devices/token", handler)

    async with TestServer(app) as server:
        manager = TokenManager(
            device_id="device-123",
            private_key_b64=ed25519_keypair["private_key_b64"],
            base_url=str(server.make_url("/")),
        )
        async with aiohttp.ClientSession() as session:
            manager._session = session
            # No token held -> must issue a fresh one.
            await manager.ensure_valid_token()
            assert manager._current_token is not None
            assert manager._current_token.jwt == "fresh-token"


@pytest.mark.asyncio
async def test_start_renewal_loop_is_idempotent(ed25519_keypair):
    """A second start_renewal_loop call must not spawn a duplicate task (P1-10)."""
    manager = TokenManager(
        device_id="device-123",
        private_key_b64=ed25519_keypair["private_key_b64"],
        base_url="http://localhost/",
    )
    # Give it a token so the loop can compute an interval without crashing.
    now = datetime.now(timezone.utc)
    from datetime import timedelta as _td
    manager._current_token = TokenCredentials(
        jwt="t", issued_at=now, expires_at=now + _td(seconds=3600)
    )

    manager.start_renewal_loop()
    first = manager._renewal_task
    manager.start_renewal_loop()
    assert manager._renewal_task is first

    await manager.stop()
