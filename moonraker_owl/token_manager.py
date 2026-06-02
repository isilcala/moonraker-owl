"""JWT token manager with Ed25519 signing."""

from __future__ import annotations

import asyncio
import base64
import binascii
import contextlib
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

import aiohttp
from cryptography.hazmat.primitives.asymmetric.ed25519 import Ed25519PrivateKey

LOGGER = logging.getLogger(__name__)

# Bounds for the server-supplied token lifetime (``expiresIn``, seconds).
MIN_TOKEN_LIFETIME_SECONDS = 60  # 1 minute (matches renewal-interval floor)
MAX_TOKEN_LIFETIME_SECONDS = 7 * 24 * 60 * 60  # 7 days
# Warn if the device clock differs from the server's issuedAt by more than this.
MAX_CLOCK_DRIFT_SECONDS = 60.0


class TokenManagerKeyError(RuntimeError):
    """Raised when the device private key cannot be decoded or loaded."""



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
        renewal_alert_after: int = 3,  # CRITICAL after N consecutive failures
    ) -> None:
        self.device_id = device_id
        self.base_url = base_url
        self.renewal_ratio = renewal_ratio
        self.safety_buffer_seconds = safety_buffer_seconds
        self.renewal_alert_after = max(1, renewal_alert_after)

        # Parse private key. A corrupt or truncated key must fail with a clear,
        # actionable message (re-link the device) rather than an opaque crash.
        try:
            private_key_bytes = base64.b64decode(private_key_b64, validate=True)
        except (binascii.Error, ValueError) as exc:
            raise TokenManagerKeyError(
                "Device private key is not valid base64; the credential file is "
                "corrupt. Re-link this device (run: moonraker-owl link)."
            ) from exc

        if len(private_key_bytes) != 32:
            raise TokenManagerKeyError(
                "Device private key must be exactly 32 bytes "
                f"(got {len(private_key_bytes)}); the credential file is corrupt. "
                "Re-link this device (run: moonraker-owl link)."
            )

        try:
            self._private_key = Ed25519PrivateKey.from_private_bytes(private_key_bytes)
        except Exception as exc:  # cryptography raises ValueError on bad keys
            raise TokenManagerKeyError(
                "Device private key could not be loaded; the credential file is "
                "corrupt. Re-link this device (run: moonraker-owl link)."
            ) from exc

        self._current_token: Optional[TokenCredentials] = None
        self._renewal_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._session: Optional[aiohttp.ClientSession] = None
        self._renewal_failures = 0

    async def start(self) -> None:
        """Start token manager and issue initial token."""
        self._session = aiohttp.ClientSession()
        self._current_token = await self.issue_token()
        LOGGER.info(
            "Initial JWT token issued, expires at %s", self._current_token.expires_at
        )

    def seconds_until_expiry(self) -> Optional[float]:
        """Seconds until the current token expires, or None if no token is held."""
        return self._time_to_expiry_seconds()

    def is_token_valid(self) -> bool:
        """Return True if a token is held and not within the safety buffer of expiry.

        Used as a startup precheck: if a slow first MQTT connect (or any delay)
        leaves the freshly-issued token already inside its safety buffer, callers
        should refresh before proceeding rather than connecting with a token that
        is about to expire.
        """
        if self._current_token is None:
            return False
        now = datetime.now(timezone.utc)
        return now < self._current_token.expires_at - timedelta(
            seconds=self.safety_buffer_seconds
        )

    async def ensure_valid_token(self) -> None:
        """Re-issue the token if it is missing or within its safety buffer."""
        if not self.is_token_valid():
            LOGGER.info("Token missing or near expiry at startup; re-issuing")
            self._current_token = await self.issue_token()

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

    def get_token(self) -> Optional[str]:
        """Get current JWT token string, or None if not initialized.

        This is a convenience method for components that need just the token,
        such as the MetadataReporter for HTTP Bearer authentication.
        """
        if not self._current_token:
            return None
        return self._current_token.jwt

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
            self._warn_on_clock_drift(issued_at)
            # expiresIn is in seconds, calculate expires_at from issuedAt.
            # Bound-check the server value: a tampered/buggy 0/negative would
            # cause an immediate-expiry hot loop hammering the token endpoint,
            # while an absurdly large value would yield a token that never
            # renews. Clamp to a sane window and reject clearly-bad values.
            expires_in_seconds = self._validate_expires_in(data.get("expiresIn"))
            expires_at = issued_at + timedelta(seconds=expires_in_seconds)

            return TokenCredentials(jwt=jwt, issued_at=issued_at, expires_at=expires_at)

    @staticmethod
    def _validate_expires_in(raw: Any) -> int:
        """Validate the server-supplied ``expiresIn`` (seconds).

        Accepts integers in [MIN_TOKEN_LIFETIME_SECONDS, MAX_TOKEN_LIFETIME_SECONDS];
        raises ``ValueError`` for missing/non-numeric/out-of-range values.
        """
        if raw is None:
            raise ValueError("Token response missing 'expiresIn'")
        try:
            value = int(raw)
        except (TypeError, ValueError) as exc:
            raise ValueError(f"Invalid 'expiresIn' value: {raw!r}") from exc
        if value < MIN_TOKEN_LIFETIME_SECONDS or value > MAX_TOKEN_LIFETIME_SECONDS:
            raise ValueError(
                f"'expiresIn' {value}s outside allowed range "
                f"[{MIN_TOKEN_LIFETIME_SECONDS}, {MAX_TOKEN_LIFETIME_SECONDS}]"
            )
        return value

    @staticmethod
    def _warn_on_clock_drift(issued_at: datetime) -> None:
        """Warn when the local clock drifts significantly from the server.

        Token renewal scheduling is driven by ``issued_at``/``expires_at`` from
        the server. A skewed local clock can make the device renew too early
        (wasteful) or too late (auth failures), so surface large drift loudly.
        """
        now = datetime.now(timezone.utc)
        drift = abs((now - issued_at).total_seconds())
        if drift > MAX_CLOCK_DRIFT_SECONDS:
            LOGGER.warning(
                "Device clock drifts %.1fs from server issuedAt "
                "(threshold %.0fs); token renewal timing may be affected. "
                "Consider enabling NTP time sync.",
                drift,
                MAX_CLOCK_DRIFT_SECONDS,
            )

    async def refresh_token_now(self) -> None:
        """Immediately refresh the JWT token (used for resilience on auth failures)."""
        LOGGER.debug("Refreshing JWT token (on-demand)")
        self._current_token = await self.issue_token()
        LOGGER.info("JWT token refreshed, expires %s", self._current_token.expires_at)

    def start_renewal_loop(
        self, on_renewed: Optional[asyncio.coroutine] = None
    ) -> None:
        """Start background task to renew token dynamically based on expiry.
        
        Renewal is scheduled at renewal_ratio (default 85%) of the token's lifetime,
        with an additional safety_buffer_seconds (default 2 minutes) subtracted.
        
        Example: 1-hour token (3600s) → renews at 3600 * 0.85 - 120 = 2940s (49 minutes)
        """
        if self._renewal_task is not None and not self._renewal_task.done():
            LOGGER.debug("Renewal loop already running; skipping duplicate start")
            return
        self._renewal_task = asyncio.create_task(self._renewal_loop(on_renewed))

    async def _renewal_loop(self, on_renewed: Optional[asyncio.coroutine]) -> None:
        """Background task that renews token dynamically based on expiry."""
        while not self._stop_event.is_set():
            # Calculate renewal interval based on current token's lifetime
            renewal_interval = self._calculate_renewal_interval()

            try:
                await asyncio.wait_for(
                    self._stop_event.wait(), timeout=renewal_interval
                )
                break  # Stop event was set
            except asyncio.TimeoutError:
                pass  # Normal timeout, proceed with renewal

            try:
                LOGGER.debug("Renewing JWT token")
                self._current_token = await self.issue_token()
                LOGGER.info("JWT token renewed, expires %s", self._current_token.expires_at)
                if self._renewal_failures:
                    LOGGER.info(
                        "JWT token renewal recovered after %d consecutive failure(s)",
                        self._renewal_failures,
                    )
                self._renewal_failures = 0

                if on_renewed:
                    await on_renewed()

            except Exception as exc:
                self._renewal_failures += 1
                time_remaining = self._time_to_expiry_seconds()
                retry_interval = self._calculate_retry_interval(
                    self._renewal_failures, time_remaining
                )
                self._log_renewal_failure(
                    exc, self._renewal_failures, time_remaining, retry_interval
                )
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(), timeout=retry_interval
                    )
                    break
                except asyncio.TimeoutError:
                    pass

    @property
    def renewal_failures(self) -> int:
        """Consecutive token-renewal failures (0 when healthy)."""

        return self._renewal_failures

    def _time_to_expiry_seconds(self) -> Optional[float]:
        """Seconds until the current token expires, or None if no token yet."""

        if not self._current_token:
            return None
        now = datetime.now(timezone.utc)
        return (self._current_token.expires_at - now).total_seconds()

    def _calculate_retry_interval(
        self, consecutive_failures: int, time_remaining: Optional[float]
    ) -> int:
        """Progressive backoff for renewal retries with critical acceleration.

        Normal backoff is exponential (30s base, doubling) capped at 1 hour. When
        the current token is close to expiry (<10 min) the interval is forced
        short so renewal keeps trying aggressively before credentials lapse.
        """

        base = 30
        cap = 3600
        interval = min(cap, base * (2 ** max(0, consecutive_failures - 1)))

        # Critical acceleration: token is about to expire, keep retrying fast.
        if time_remaining is not None and time_remaining <= 600:
            interval = min(interval, 30)
        # Token already expired or unknown lifetime — retry promptly.
        if time_remaining is not None and time_remaining <= 0:
            interval = 15

        return max(15, int(interval))

    def _log_renewal_failure(
        self,
        exc: BaseException,
        consecutive_failures: int,
        time_remaining: Optional[float],
        retry_interval: int,
    ) -> None:
        remaining_text = (
            f"{time_remaining:.0f}s to expiry"
            if time_remaining is not None
            else "no active token"
        )
        if consecutive_failures >= self.renewal_alert_after:
            LOGGER.critical(
                "JWT token renewal failing: %d consecutive failures (%s); "
                "retrying in %ds. Last error: %s",
                consecutive_failures,
                remaining_text,
                retry_interval,
                exc,
            )
        else:
            LOGGER.error(
                "Token renewal failed (#%d, %s); retrying in %ds: %s",
                consecutive_failures,
                remaining_text,
                retry_interval,
                exc,
            )

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

        return interval

    def _sign_timestamp(self, timestamp: int) -> str:
        """Sign timestamp with Ed25519 private key."""
        message = f"{self.device_id}:{timestamp}"
        message_bytes = message.encode("utf-8")
        signature_bytes = self._private_key.sign(message_bytes)
        return base64.b64encode(signature_bytes).decode("ascii")
