"""JWT token manager with Ed25519 signing."""

from __future__ import annotations

import asyncio
import base64
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Optional

import aiohttp
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

LOGGER = logging.getLogger(__name__)


@dataclass(slots=True)
class TokenCredentials:
    """JWT token with metadata."""

    jwt: str
    expires_at: datetime
    issued_at: datetime


class TokenManager:
    """Manages device JWT tokens with automatic renewal."""

    def __init__(
        self,
        *,
        device_id: str,
        private_key_b64: str,
        base_url: str,
        renewal_ratio: float = 0.85,  # Renew at 85% of token lifetime
        safety_buffer_seconds: int = 120,  # 2-minute safety buffer
    ) -> None:
        self.device_id = device_id
        self.base_url = base_url
        self.renewal_ratio = renewal_ratio
        self.safety_buffer_seconds = safety_buffer_seconds

        # Parse private key
        private_key_bytes = base64.b64decode(private_key_b64)
        self._private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)

        self._current_token: Optional[TokenCredentials] = None
        self._renewal_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._session: Optional[aiohttp.ClientSession] = None

    async def start(self) -> None:
        """Start token manager and issue initial token."""
        self._session = aiohttp.ClientSession()
        self._current_token = await self.issue_token()
        LOGGER.info(
            "Initial JWT token issued, expires at %s", self._current_token.expires_at
        )

    async def stop(self) -> None:
        """Stop renewal loop and cleanup resources."""
        self._stop_event.set()
        if self._renewal_task:
            self._renewal_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._renewal_task
            self._renewal_task = None
        if self._session:
            await self._session.close()
            self._session = None

    def get_mqtt_credentials(self) -> tuple[str, str]:
        """Get current MQTT credentials: (device_id, jwt_token).

        Returns device_id as both username and clientId for EMQX JWT validation.
        EMQX will verify that the JWT's 'sub' claim matches the MQTT username.
        """
        if not self._current_token:
            raise RuntimeError("Token not initialized. Call start() first.")
        return self.device_id, self._current_token.jwt

    async def issue_token(self) -> TokenCredentials:
        """Issue a new JWT token by signing current timestamp."""
        timestamp = int(datetime.now(timezone.utc).timestamp())
        signature = self._sign_timestamp(timestamp)

        url = f"{self.base_url.rstrip('/')}/api/v1/devices/token"
        payload = {
            "deviceId": self.device_id,
            "timestamp": timestamp,
            "signature": signature,
        }

        LOGGER.debug("Requesting JWT token from %s", url)

        if not self._session:
            raise RuntimeError("Session not initialized")

        async with self._session.post(
            url, json=payload, timeout=aiohttp.ClientTimeout(total=10.0)
        ) as response:
            if response.status == 401:
                raise RuntimeError(
                    "Token request rejected: invalid signature or timestamp"
                )
            if response.status == 404:
                raise RuntimeError(
                    "Token request rejected: device not found or key revoked"
                )
            if response.status == 429:
                raise RuntimeError("Token request rejected: rate limit exceeded")
            if response.status != 200:
                text = await response.text()
                raise RuntimeError(
                    f"Token request failed with status {response.status}: {text}"
                )

            data = await response.json()
            jwt = data["accessToken"]
            issued_at = datetime.fromisoformat(data["issuedAt"].replace("Z", "+00:00"))
            # expiresIn is in seconds, calculate expires_at from issuedAt
            expires_in_seconds = data["expiresIn"]
            expires_at = issued_at + timedelta(seconds=expires_in_seconds)

            return TokenCredentials(jwt=jwt, issued_at=issued_at, expires_at=expires_at)

    async def refresh_token_now(self) -> None:
        """Immediately refresh the JWT token (used for resilience on auth failures)."""
        LOGGER.info("Refreshing JWT token immediately (on-demand)")
        self._current_token = await self.issue_token()
        LOGGER.info(
            "JWT token refreshed on-demand, expires at %s",
            self._current_token.expires_at,
        )

    def start_renewal_loop(
        self, on_renewed: Optional[asyncio.coroutine] = None
    ) -> None:
        """Start background task to renew token dynamically based on expiry.
        
        Renewal is scheduled at renewal_ratio (default 85%) of the token's lifetime,
        with an additional safety_buffer_seconds (default 2 minutes) subtracted.
        
        Example: 1-hour token (3600s) → renews at 3600 * 0.85 - 120 = 2940s (49 minutes)
        """
        self._renewal_task = asyncio.create_task(self._renewal_loop(on_renewed))

    async def _renewal_loop(self, on_renewed: Optional[asyncio.coroutine]) -> None:
        """Background task that renews token dynamically based on expiry."""
        while not self._stop_event.is_set():
            # Calculate renewal interval based on current token's lifetime
            renewal_interval = self._calculate_renewal_interval()
            
            LOGGER.debug(
                "Token renewal scheduled in %d seconds (%.1f minutes)",
                renewal_interval,
                renewal_interval / 60.0
            )

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=renewal_interval
                )
                break  # Stop event was set
            except asyncio.TimeoutError:
                pass  # Normal timeout, proceed with renewal

            try:
                LOGGER.info("Renewing JWT token")
                self._current_token = await self.issue_token()
                LOGGER.info(
                    "JWT token renewed, expires at %s", self._current_token.expires_at
                )

                if on_renewed:
                    await on_renewed()

            except Exception as exc:
                LOGGER.error("Token renewal failed: %s", exc, exc_info=True)
                # On failure, retry after 5 minutes instead of full interval
                retry_interval = 300
                LOGGER.info("Will retry token renewal in %d seconds", retry_interval)
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=retry_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass

    def _calculate_renewal_interval(self) -> int:
        """Calculate when to renew token based on its expiry time.
        
        Returns interval in seconds until renewal should occur.
        Uses renewal_ratio (default 85%) of token lifetime minus safety buffer.
        Minimum interval is 60 seconds.
        """
        if not self._current_token:
            # No token yet, shouldn't happen but fallback to 1 minute
            return 60

        now = datetime.now(timezone.utc)
        token_lifetime = (self._current_token.expires_at - self._current_token.issued_at).total_seconds()
        time_elapsed = (now - self._current_token.issued_at).total_seconds()
        time_remaining = (self._current_token.expires_at - now).total_seconds()

        # Calculate renewal point: renewal_ratio of lifetime, minus safety buffer
        renewal_point = (token_lifetime * self.renewal_ratio) - self.safety_buffer_seconds
        
        # Time until renewal = renewal_point - time_elapsed
        interval = renewal_point - time_elapsed

        # Ensure minimum interval of 60 seconds
        interval = max(60, int(interval))

        LOGGER.debug(
            "Token renewal calculation: lifetime=%ds, elapsed=%ds, remaining=%ds, "
            "renewal_ratio=%.2f, safety_buffer=%ds, calculated_interval=%ds",
            int(token_lifetime), int(time_elapsed), int(time_remaining),
            self.renewal_ratio, self.safety_buffer_seconds, interval
        )

        return interval

    def _sign_timestamp(self, timestamp: int) -> str:
        """Sign timestamp with Ed25519 private key."""
        message = f"{self.device_id}:{timestamp}"
        message_bytes = message.encode("utf-8")
        signature_bytes = self._private_key.sign(message_bytes)
        return base64.b64encode(signature_bytes).decode("ascii")
