"""Cloud configuration manager with ETag-based caching and LKG persistence.

Responsible for fetching Tier C configuration from the cloud API,
caching via Last-Known-Good (LKG) files, and notifying registered
callbacks when configuration changes.

See ADR-0040 §Decision 4 for transport design.
"""

from __future__ import annotations

import asyncio
import json
import logging
import random
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Awaitable, Callable, Dict, List, Optional

import aiohttp

from . import constants
from .config import (
    CameraConfig,
    CommandConfig,
    CompressionConfig,
    OwlConfig,
    TelemetryCadenceConfig,
    TelemetryConfig,
)
from .version import __version__

LOGGER = logging.getLogger(__name__)

# How long after a successful fetch before we skip on token-refresh triggers
_TTL_SKIP_SECONDS = 30 * 60  # 30 minutes

# Maximum age of a LKG file before we warn (but still use it)
_LKG_STALE_HOURS = 72

ConfigChangeCallback = Callable[[OwlConfig], Awaitable[None]]


@dataclass(slots=True)
class CloudConfigState:
    """Runtime state for cloud config tracking."""

    etag: Optional[str] = None
    last_fetched_at: float = 0.0  # monotonic timestamp
    last_fetched_utc: Optional[str] = None  # ISO 8601 for LKG meta


def _camel_to_snake(name: str) -> str:
    """Convert camelCase to snake_case."""
    result: list[str] = []
    for i, ch in enumerate(name):
        if ch.isupper() and i > 0:
            result.append("_")
        result.append(ch.lower())
    return "".join(result)


def _snake_dict(d: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively convert camelCase keys to snake_case."""
    out: Dict[str, Any] = {}
    for k, v in d.items():
        sk = _camel_to_snake(k)
        if isinstance(v, dict):
            out[sk] = _snake_dict(v)
        else:
            out[sk] = v
    return out


def apply_cloud_config(config: OwlConfig, cloud_data: Dict[str, Any]) -> None:
    """Apply cloud-fetched Tier C configuration onto the OwlConfig in-place.

    ``cloud_data`` should be the JSON body from GET /devices/{id}/config
    with keys already converted to snake_case.
    """
    tel = cloud_data.get("telemetry")
    if isinstance(tel, dict):
        tc = config.telemetry
        if "status_interval_seconds" in tel:
            tc.rate_hz = 1.0 / max(tel["status_interval_seconds"], 0.1)
        if "sensors_interval_seconds" in tel:
            # stored for use by cadence controller, not directly on TelemetryConfig
            pass
        if "include_fields" in tel:
            tc.include_fields = list(tel["include_fields"])
        if "exclude_fields" in tel:
            tc.exclude_fields = list(tel["exclude_fields"])
        if "include_raw_payload" in tel:
            tc.include_raw_payload = bool(tel["include_raw_payload"])
        if "sensor_allowlist" in tel:
            tc.sensor_allowlist = list(tel["sensor_allowlist"])
        if "sensor_denylist" in tel:
            tc.sensor_denylist = list(tel["sensor_denylist"])

    cad = cloud_data.get("cadence")
    if isinstance(cad, dict):
        cc = config.telemetry_cadence
        for fname in (
            "status_heartbeat_seconds",
            "status_idle_interval_seconds",
            "status_active_interval_seconds",
            "sensors_force_publish_seconds",
            "events_max_per_second",
            "events_max_per_minute",
            "thumbnail_fetch_timeout_ms",
            "timelapse_poll_interval_seconds",
        ):
            if fname in cad:
                setattr(cc, fname, type(getattr(cc, fname))(cad[fname]))

    cam = cloud_data.get("camera")
    if isinstance(cam, dict):
        cc = config.camera
        for fname in (
            "enabled",
            "snapshot_url",
            "camera_name",
            "capture_timeout_seconds",
            "max_retries",
            "preprocess_enabled",
            "preprocess_target_width",
            "preprocess_jpeg_quality",
        ):
            if fname in cam:
                setattr(cc, fname, type(getattr(cc, fname))(cam[fname]))

    comp = cloud_data.get("compression")
    if isinstance(comp, dict):
        cc = config.compression
        if "enabled" in comp:
            cc.enabled = bool(comp["enabled"])
        if "channels" in comp:
            cc.channels = list(comp["channels"])
        if "min_size_bytes" in comp:
            cc.min_size_bytes = int(comp["min_size_bytes"])

    cmds = cloud_data.get("commands")
    if isinstance(cmds, dict):
        if "ack_timeout_seconds" in cmds:
            config.commands.ack_timeout_seconds = float(cmds["ack_timeout_seconds"])


class CloudConfigManager:
    """Manages cloud configuration with ETag caching and LKG persistence.

    Lifecycle:
        1. ``load_lkg()`` — read LKG cache to prime OwlConfig (cold start Layer 2)
        2. ``fetch()`` — HTTP GET with If-None-Match (Layer 3)
        3. ``schedule_fetch(jitter_seconds)`` — deferred fetch after MQTT notification
    """

    def __init__(
        self,
        *,
        config: OwlConfig,
        device_id: str,
        base_url: str,
        token_provider: Callable[[], Optional[str]],
        lkg_path: Optional[Path] = None,
        http_session: Optional[aiohttp.ClientSession] = None,
    ) -> None:
        self._config = config
        self._device_id = device_id
        self._base_url = base_url.rstrip("/")
        self._token_provider = token_provider
        self._lkg_path = lkg_path or constants.DEFAULT_CLOUD_CONFIG_PATH
        self._session = http_session
        self._owns_session = http_session is None

        self._state = CloudConfigState()
        self._callbacks: List[ConfigChangeCallback] = []
        self._pending_fetch_task: Optional[asyncio.Task[None]] = None
        self._stopped = False

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register_callback(self, cb: ConfigChangeCallback) -> None:
        """Register a callback to be invoked when cloud config changes."""
        self._callbacks.append(cb)

    def load_lkg(self) -> bool:
        """Load Last-Known-Good cache from disk into OwlConfig.

        Returns True if a valid LKG was loaded.
        """
        if not self._lkg_path.exists():
            LOGGER.debug("No LKG cache at %s", self._lkg_path)
            return False

        try:
            with self._lkg_path.open("r", encoding="utf-8") as f:
                data = json.load(f)
        except (json.JSONDecodeError, OSError) as exc:
            LOGGER.warning("Failed to read LKG cache: %s", exc)
            return False

        meta = data.get("_meta", {})
        etag = meta.get("etag")
        fetched_at = meta.get("fetchedAt")

        # Warn if stale but still use it
        if fetched_at:
            try:
                fetched = datetime.fromisoformat(fetched_at)
                age_hours = (datetime.now(timezone.utc) - fetched).total_seconds() / 3600
                if age_hours > _LKG_STALE_HOURS:
                    LOGGER.warning(
                        "LKG cache is %.0f hours old (limit %d); using anyway",
                        age_hours,
                        _LKG_STALE_HOURS,
                    )
            except (ValueError, TypeError):
                pass

        # Strip meta before applying
        config_data = {k: v for k, v in data.items() if k != "_meta"}
        snake_data = _snake_dict(config_data)
        apply_cloud_config(self._config, snake_data)

        if etag:
            self._state.etag = etag
        LOGGER.info("Loaded LKG cloud config (etag=%s)", etag or "none")
        return True

    async def fetch(self, *, force: bool = False) -> bool:
        """Fetch cloud config via HTTP GET with ETag.

        Returns True if config was updated (200), False if unchanged (304)
        or on error.

        If *force* is False and a recent fetch occurred within _TTL_SKIP_SECONDS,
        the request is skipped to avoid unnecessary 304s during token renewal.
        """
        if self._stopped:
            return False

        # TTL skip logic
        if not force and self._state.last_fetched_at > 0:
            elapsed = time.monotonic() - self._state.last_fetched_at
            if elapsed < _TTL_SKIP_SECONDS:
                LOGGER.debug(
                    "Skipping cloud config fetch (%.0fs since last fetch, TTL %ds)",
                    elapsed,
                    _TTL_SKIP_SECONDS,
                )
                return False

        token = self._token_provider()
        if not token:
            LOGGER.warning("No JWT token available; skipping cloud config fetch")
            return False

        url = f"{self._base_url}/api/v1/devices/{self._device_id}/config"
        headers: Dict[str, str] = {"Authorization": f"Bearer {token}"}
        if self._state.etag:
            headers["If-None-Match"] = self._state.etag

        session = self._session
        if session is None:
            session = aiohttp.ClientSession()
            self._session = session

        try:
            async with session.get(
                url, headers=headers, timeout=aiohttp.ClientTimeout(total=15.0)
            ) as resp:
                if resp.status == 304:
                    LOGGER.debug("Cloud config unchanged (304)")
                    self._state.last_fetched_at = time.monotonic()
                    self._state.last_fetched_utc = datetime.now(timezone.utc).isoformat()
                    return False

                if resp.status == 200:
                    data = await resp.json()
                    new_etag = resp.headers.get("ETag")

                    snake_data = _snake_dict(data)
                    apply_cloud_config(self._config, snake_data)

                    self._state.etag = new_etag
                    self._state.last_fetched_at = time.monotonic()
                    self._state.last_fetched_utc = datetime.now(timezone.utc).isoformat()

                    self._write_lkg(data, new_etag)

                    LOGGER.info("Cloud config updated (etag=%s)", new_etag)
                    await self._fire_callbacks()
                    return True

                LOGGER.warning(
                    "Cloud config fetch failed (status=%d)", resp.status
                )
                return False

        except (aiohttp.ClientError, asyncio.TimeoutError) as exc:
            LOGGER.warning("Cloud config fetch error: %s", exc)
            return False

    def schedule_fetch(self, jitter_seconds: float = 0) -> None:
        """Schedule a deferred fetch with jitter, coalescing multiple calls.

        If a fetch is already pending, the existing timer is cancelled and
        a new one is started (coalescing successive MQTT notifications).
        """
        if self._stopped:
            return

        # Cancel any existing pending fetch
        if self._pending_fetch_task is not None and not self._pending_fetch_task.done():
            self._pending_fetch_task.cancel()

        delay = random.uniform(0, max(jitter_seconds, 0)) if jitter_seconds > 0 else 0
        LOGGER.debug("Scheduling cloud config fetch in %.1fs", delay)
        self._pending_fetch_task = asyncio.create_task(self._deferred_fetch(delay))

    async def stop(self) -> None:
        """Cancel pending fetches and clean up resources."""
        self._stopped = True
        if self._pending_fetch_task is not None and not self._pending_fetch_task.done():
            self._pending_fetch_task.cancel()
            try:
                await self._pending_fetch_task
            except asyncio.CancelledError:
                pass
            self._pending_fetch_task = None
        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    async def _deferred_fetch(self, delay: float) -> None:
        """Wait *delay* seconds then fetch.  Used by schedule_fetch."""
        try:
            if delay > 0:
                await asyncio.sleep(delay)
            await self.fetch(force=True)
        except asyncio.CancelledError:
            pass

    def _write_lkg(self, raw_data: Dict[str, Any], etag: Optional[str]) -> None:
        """Write LKG cache file with metadata."""
        lkg = dict(raw_data)
        lkg["_meta"] = {
            "etag": etag,
            "fetchedAt": datetime.now(timezone.utc).isoformat(),
            "ttlHours": _LKG_STALE_HOURS,
            "agentVersion": __version__,
        }
        try:
            self._lkg_path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self._lkg_path.with_suffix(".tmp")
            with tmp.open("w", encoding="utf-8") as f:
                json.dump(lkg, f, indent=2)
            tmp.replace(self._lkg_path)
            LOGGER.debug("LKG cache written to %s", self._lkg_path)
        except OSError as exc:
            LOGGER.warning("Failed to write LKG cache: %s", exc)

    async def _fire_callbacks(self) -> None:
        """Invoke all registered change callbacks."""
        for cb in self._callbacks:
            try:
                await cb(self._config)
            except Exception:
                LOGGER.exception("Cloud config change callback failed")
