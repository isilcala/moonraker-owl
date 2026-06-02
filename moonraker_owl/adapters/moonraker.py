"""Moonraker adapter providing HTTP and WebSocket helpers."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
import random
import time
from typing import Mapping, Optional
from urllib.parse import urlparse, urlunparse

import aiohttp

from ..config import MoonrakerConfig
from ..core import CallbackType, PrinterAdapter

LOGGER = logging.getLogger(__name__)


class MoonrakerClient(PrinterAdapter):
    """Non-blocking utility for Moonraker connectivity."""

    def __init__(
        self,
        config: MoonrakerConfig,
        *,
        session: Optional[aiohttp.ClientSession] = None,
        reconnect_initial: float = 1.0,
        reconnect_max: float = 30.0,
        outage_alert_after: int = 5,
        dispatch_timeout: float = 5.0,
    ) -> None:
        self.config = config
        self.reconnect_initial = reconnect_initial
        self.reconnect_max = reconnect_max
        # After this many consecutive websocket failures the outage is escalated
        # from a routine WARNING to a CRITICAL log so a prolonged Moonraker
        # disconnect is unmistakable in the diagnostics.
        self.outage_alert_after = max(1, outage_alert_after)
        # Per-callback timeout in the websocket dispatch loop so a single slow
        # handler cannot stall the whole telemetry pump.
        self._dispatch_timeout = max(0.1, dispatch_timeout)

        self._base_url = self.config.url.rstrip("/")
        self._headers = {}
        if self.config.api_key:
            self._headers["X-Api-Key"] = self.config.api_key

        self._session: Optional[aiohttp.ClientSession] = session
        self._owns_session = session is None
        self._callbacks: list[CallbackType] = []
        self._listener_task: Optional[asyncio.Task[None]] = None
        self._stop_event = asyncio.Event()
        self._subscription_objects: Optional[dict[str, Optional[list[str]]]] = None
        self._rpc_id = 0
        self._rpc_id_lock = asyncio.Lock()
        self._active_ws: Optional[aiohttp.ClientWebSocketResponse] = None
        self._ws_connected = False
        self._consecutive_failures = 0
        self._outage_started_at: Optional[float] = None
        self._outage_alerted = False

    @property
    def is_connected(self) -> bool:
        """Whether the websocket is currently established."""

        return self._ws_connected

    @property
    def consecutive_failures(self) -> int:
        """Number of consecutive websocket connection failures."""

        return self._consecutive_failures

    @property
    def outage_seconds(self) -> float:
        """Seconds since the current websocket outage began (0.0 when healthy)."""

        started = self._outage_started_at
        if started is None:
            return 0.0
        return max(0.0, time.monotonic() - started)

    def _redact(self, text: str) -> str:
        """Redact the Moonraker API key from arbitrary text (e.g. error bodies).

        Moonraker (or a misbehaving proxy) can echo request headers/values back
        in an error response. Scrub the configured API key before it reaches a
        log line or a raised exception message (P2 security hardening).
        """
        api_key = self.config.api_key
        if api_key and api_key in text:
            return text.replace(api_key, "***REDACTED***")
        return text


    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    async def start(self, callback: CallbackType) -> None:
        """Start listening to Moonraker WebSocket notifications."""

        if callback in self._callbacks:
            raise ValueError("Callback already registered")

        self._callbacks.append(callback)

        if self._listener_task is not None:
            return

        if self._owns_session and self._session is None:
            timeout = aiohttp.ClientTimeout(total=None)
            self._session = aiohttp.ClientSession(timeout=timeout)

        self._stop_event.clear()
        self._listener_task = asyncio.create_task(self._listen_loop())
        await asyncio.sleep(0)

    async def stop(self) -> None:
        """Stop listening and close the underlying resources."""

        self._callbacks.clear()
        self._stop_event.set()

        if self._listener_task is not None:
            self._listener_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._listener_task
            self._listener_task = None

        if self._owns_session and self._session is not None:
            await self._session.close()
            self._session = None

    async def aclose(self) -> None:  # alias for explicit closing
        await self.stop()

    def remove_callback(self, callback: CallbackType) -> None:
        """Remove a previously registered callback."""

        with contextlib.suppress(ValueError):
            self._callbacks.remove(callback)

    async def fetch_printer_state(
        self,
        objects: Optional[Mapping[str, Optional[list[str]]]] = None,
        timeout: float = 5.0,
    ) -> dict:
        """Fetch the current printer state via HTTP.

        Args:
            objects: Moonraker objects to query (None = all subscribed objects)
            timeout: Request timeout in seconds (default: 5.0)

        Raises:
            asyncio.TimeoutError: If request exceeds timeout
            aiohttp.ClientError: If HTTP request fails
        """

        session = await self._ensure_session()
        url = f"{self._base_url}/printer/objects/query"
        payload = {"objects": objects or {}}

        try:
            async with asyncio.timeout(timeout):
                async with session.post(
                    url, json=payload, headers=self._headers
                ) as response:
                    response.raise_for_status()
                    return await response.json()
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Moonraker query timed out after %.1fs (url=%s, objects=%d)",
                timeout,
                url,
                len(objects) if objects else 0,
            )
            raise

    async def fetch_available_heaters(self, timeout: float = 5.0) -> dict[str, list[str]]:
        """Fetch all available heaters and temperature sensors from Moonraker.

        This queries the 'heaters' object to discover all configured heating
        elements and temperature sensors, enabling dynamic subscription.

        Returns:
            Dictionary with keys:
                - 'available_heaters': List of heater object names (extruder, heater_bed, heater_generic xxx)
                - 'available_sensors': List of sensor object names (temperature_sensor xxx, temperature_fan xxx, etc.)

        Raises:
            asyncio.TimeoutError: If request exceeds timeout
            aiohttp.ClientError: If HTTP request fails
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/printer/objects/query"
        payload = {"objects": {"heaters": None}}

        try:
            async with asyncio.timeout(timeout):
                async with session.post(
                    url, json=payload, headers=self._headers
                ) as response:
                    response.raise_for_status()
                    data = await response.json()

            result = data.get("result", {})
            status = result.get("status", {})
            heaters_data = status.get("heaters", {})

            return {
                "available_heaters": heaters_data.get("available_heaters", []),
                "available_sensors": heaters_data.get("available_sensors", []),
            }
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Fetching available heaters timed out after %.1fs",
                timeout,
            )
            raise
        except (OSError, KeyError) as exc:
            LOGGER.warning("Failed to fetch available heaters: %s", exc)
            return {"available_heaters": [], "available_sensors": []}

    async def fetch_registered_objects(self, timeout: float = 5.0) -> list[str]:
        """Fetch the list of all registered Moonraker printer objects.

        Returns object names like 'neopixel my_led', 'filament_switch_sensor runout',
        'output_pin my_pin', etc.  Used for discovering non-thermal devices.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/printer/objects/list"

        try:
            async with asyncio.timeout(timeout):
                async with session.get(url, headers=self._headers) as response:
                    response.raise_for_status()
                    data = await response.json()

            return data.get("result", {}).get("objects", [])
        except asyncio.TimeoutError:
            LOGGER.warning("Fetching registered objects timed out after %.1fs", timeout)
            return []
        except (OSError, KeyError) as exc:
            LOGGER.warning("Failed to fetch registered objects: %s", exc)
            return []

    def set_subscription_objects(
        self, objects: Mapping[str, Optional[list[str]]] | None
    ) -> None:
        """Declare which Moonraker objects should be subscribed to on connect."""

        if objects is None:
            self._subscription_objects = None
        else:
            # ensure payload is JSON-serialisable and lists are copied
            self._subscription_objects = {
                key: (list(value) if isinstance(value, list) else None)
                for key, value in objects.items()
            }

    async def execute_print_action(self, action: str) -> None:
        """Invoke pause/resume/cancel actions on Moonraker."""

        action_normalized = action.strip().lower()
        endpoint = {
            "pause": "pause",
            "resume": "resume",
            "cancel": "cancel",
        }.get(action_normalized)

        if endpoint is None:
            raise ValueError(f"Unsupported Moonraker action: {action!r}")

        session = await self._ensure_session()
        url = f"{self._base_url}/printer/print/{endpoint}"

        async with session.post(url, headers=self._headers) as response:
            if response.status >= 400:
                detail = self._redact((await response.text()).strip())
                raise RuntimeError(
                    f"Moonraker action '{action_normalized}' failed with status {response.status}: {detail}"
                )

    async def emergency_stop(self) -> None:
        """Execute emergency stop on the printer.

        This calls the Moonraker emergency_stop endpoint which immediately
        halts all printer operations.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/printer/emergency_stop"

        async with session.post(url, headers=self._headers) as response:
            if response.status >= 400:
                detail = self._redact((await response.text()).strip())
                raise RuntimeError(
                    f"Emergency stop failed with status {response.status}: {detail}"
                )

    async def firmware_restart(self) -> None:
        """Restart the Klipper firmware.

        This calls the Moonraker firmware_restart endpoint which restarts
        the Klipper MCU firmware and reloads the configuration.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/printer/firmware_restart"

        async with session.post(url, headers=self._headers) as response:
            if response.status >= 400:
                detail = self._redact((await response.text()).strip())
                raise RuntimeError(
                    f"Firmware restart failed with status {response.status}: {detail}"
                )

    async def start_print(self, filename: str) -> None:
        """Start printing the specified GCode file.

        Args:
            filename: The GCode filename to print.

        Raises:
            ValueError: If filename is empty.
            RuntimeError: If the print start fails.
        """
        if not filename or not filename.strip():
            raise ValueError("Filename cannot be empty")

        session = await self._ensure_session()
        url = f"{self._base_url}/printer/print/start"
        params = {"filename": filename.strip()}

        async with session.post(url, params=params, headers=self._headers) as response:
            if response.status >= 400:
                detail = self._redact((await response.text()).strip())
                raise RuntimeError(
                    f"Start print failed with status {response.status}: {detail}"
                )

    async def execute_gcode(
        self,
        script: str,
        timeout: float = 10.0,
        fire_and_forget: bool = False,
    ) -> None:
        """Execute a GCode script via Moonraker HTTP API.

        Args:
            script: GCode command(s) to execute. Multiple commands can be
                    separated by newlines.
            timeout: Request timeout in seconds (default: 10.0).
            fire_and_forget: If True, send the request without waiting for
                    completion. Useful for commands where state changes are
                    tracked via WebSocket notifications (e.g., EXCLUDE_OBJECT).

        Raises:
            asyncio.TimeoutError: If request exceeds timeout (only when not fire_and_forget).
            RuntimeError: If GCode execution fails (only when not fire_and_forget).
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/printer/gcode/script"
        payload = {"script": script}

        if fire_and_forget:
            # Fire-and-forget: just send the request, don't wait for response
            # State changes will be tracked via Moonraker WebSocket notifications
            try:
                async with asyncio.timeout(5.0):  # Short timeout just for sending
                    async with session.post(
                        url, json=payload, headers=self._headers
                    ) as response:
                        if response.status >= 400:
                            detail = self._redact((await response.text()).strip())
                            LOGGER.warning(
                                "GCode send failed with status %d: %s (script=%r)",
                                response.status,
                                detail,
                                script[:50] + "..." if len(script) > 50 else script,
                            )
                            raise RuntimeError(
                                f"GCode send failed with status {response.status}: {detail}"
                            )
                        LOGGER.debug("GCode sent (fire-and-forget): %r", script)
            except asyncio.TimeoutError:
                # For fire-and-forget, timeout on send is a real error
                LOGGER.warning(
                    "GCode send timed out after 5.0s (script=%r)",
                    script[:50] + "..." if len(script) > 50 else script,
                )
                raise
            return

        # Standard blocking execution
        try:
            async with asyncio.timeout(timeout):
                async with session.post(
                    url, json=payload, headers=self._headers
                ) as response:
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        raise RuntimeError(
                            f"GCode execution failed with status {response.status}: {detail}"
                        )
        except asyncio.TimeoutError:
            LOGGER.warning(
                "GCode execution timed out after %.1fs (script=%r)",
                timeout,
                script[:50] + "..." if len(script) > 50 else script,
            )
            raise

    async def list_gcode_files(self, timeout: float = 15.0) -> list[dict]:
        """List GCode files from Moonraker file manager.

        Returns a flat recursive list of dicts with 'path', 'modified', 'size' fields.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/server/files/list?root=gcodes"

        async with asyncio.timeout(timeout):
            async with session.get(url, headers=self._headers) as response:
                response.raise_for_status()
                data = await response.json()
                return data.get("result", [])

    async def fetch_thumbnail(
        self,
        relative_path: str,
        gcode_filename: Optional[str] = None,
        timeout: float = 30.0,
    ) -> Optional[bytes]:
        """Fetch thumbnail image data from Moonraker file server.

        Moonraker serves thumbnails via /server/files/gcodes/{path} where the path
        is the relative path from gcode_metadata.thumbnails[].relative_path,
        prefixed with the gcode file's subdirectory if any.

        For example:
            - gcode_filename: "4N/model.gcode"
            - relative_path: ".thumbs/model-300x300.png"
            - full_path: "4N/.thumbs/model-300x300.png"

        This matches Mainsail's thumbnail URL construction logic.

        Args:
            relative_path: Relative path to the thumbnail (e.g., ".thumbs/model-32x32.png")
            gcode_filename: The gcode filename, used to extract subdirectory prefix.
            timeout: Request timeout in seconds (default: 30.0)

        Returns:
            Raw image bytes if successful, None if thumbnail not found.

        Raises:
            asyncio.TimeoutError: If request exceeds timeout.
            RuntimeError: If fetch fails with non-404 error.
        """
        session = await self._ensure_session()

        # Extract subdirectory from gcode filename if present (Mainsail pattern)
        # For "4N/model.gcode", subdirectory is "4N"
        # For "model.gcode", subdirectory is None
        full_path = relative_path
        if gcode_filename and "/" in gcode_filename:
            subdirectory = gcode_filename.rsplit("/", 1)[0]
            full_path = f"{subdirectory}/{relative_path}"

        # URL encode the path but preserve slashes
        from urllib.parse import quote

        encoded_path = quote(full_path, safe="/")
        url = f"{self._base_url}/server/files/gcodes/{encoded_path}"

        try:
            async with asyncio.timeout(timeout):
                async with session.get(url, headers=self._headers) as response:
                    if response.status == 404:
                        LOGGER.debug("Thumbnail not found: %s (url=%s)", full_path, url)
                        return None
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        raise RuntimeError(
                            f"Thumbnail fetch failed with status {response.status}: {detail}"
                        )
                    return await response.read()
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Thumbnail fetch timed out after %.1fs (path=%s)",
                timeout,
                full_path,
            )
            raise

    async def fetch_gcode_metadata(
        self, filename: str, timeout: float = 10.0
    ) -> Optional[dict]:
        """Fetch GCode file metadata including thumbnail paths.

        Args:
            filename: The GCode filename (e.g., "model.gcode")
            timeout: Request timeout in seconds (default: 10.0)

        Returns:
            Metadata dictionary containing thumbnails info, or None if not found.
            Example: {
                "filename": "model.gcode",
                "thumbnails": [
                    {"relative_path": ".thumbs/model-32x32.png", "width": 32, "height": 32},
                    {"relative_path": ".thumbs/model-300x300.png", "width": 300, "height": 300}
                ],
                ...
            }

        Raises:
            asyncio.TimeoutError: If request exceeds timeout.
        """
        session = await self._ensure_session()

        from urllib.parse import quote
        encoded_filename = quote(filename, safe="")
        url = f"{self._base_url}/server/files/metadata?filename={encoded_filename}"

        try:
            async with asyncio.timeout(timeout):
                async with session.get(url, headers=self._headers) as response:
                    if response.status == 404:
                        LOGGER.debug("GCode metadata not found: %s", filename)
                        return None
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        LOGGER.warning(
                            "Failed to fetch gcode metadata for %s: %s",
                            filename,
                            detail[:200],
                        )
                        return None
                    data = await response.json()
                    return data.get("result")
        except asyncio.TimeoutError:
            LOGGER.warning(
                "GCode metadata fetch timed out after %.1fs (file=%s)",
                timeout,
                filename,
            )
            raise
        except (OSError, KeyError, ValueError) as exc:
            LOGGER.warning(
                "Failed to parse gcode metadata for %s: %s",
                filename,
                exc,
            )
            return None

    async def fetch_most_recent_job(
        self, timeout: float = 5.0
    ) -> Optional[dict]:
        """Fetch the most recent job from Moonraker's history API.

        This is used to reliably obtain moonrakerJobId when a print starts,
        as WebSocket notifications may arrive after print state change.
        Follows the Obico pattern of querying history API for job correlation.

        Args:
            timeout: Request timeout in seconds (default: 5.0)

        Returns:
            Job dict containing 'job_id', 'filename', 'start_time', etc.
            or None if no jobs in history.

        Raises:
            asyncio.TimeoutError: If request exceeds timeout.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/server/history/list"
        params = {"limit": "1", "order": "desc"}

        try:
            async with asyncio.timeout(timeout):
                async with session.get(
                    url, params=params, headers=self._headers
                ) as response:
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        LOGGER.warning(
                            "Failed to fetch history list: %s",
                            detail[:200],
                        )
                        return None
                    data = await response.json()
                    jobs = data.get("result", {}).get("jobs", [])
                    if jobs:
                        return jobs[0]
                    return None
        except asyncio.TimeoutError:
            LOGGER.warning(
                "History list fetch timed out after %.1fs",
                timeout,
            )
            raise
        except (OSError, KeyError, ValueError) as exc:
            LOGGER.warning("Failed to parse history response: %s", exc)
            return None

    async def list_timelapse_files(
        self,
        timeout: float = 10.0,
    ) -> list[dict[str, Any]]:
        """List timelapse files from Moonraker file manager.

        Queries the 'timelapse' root in file_manager to get a list of all
        timelapse files (videos and thumbnails).

        Args:
            timeout: Request timeout in seconds (default: 10.0)

        Returns:
            List of file info dicts, each containing:
                - path: Filename
                - modified: Unix timestamp of last modification
                - size: File size in bytes

        Raises:
            asyncio.TimeoutError: If request exceeds timeout.
            RuntimeError: If query fails.
        """
        session = await self._ensure_session()
        url = f"{self._base_url}/server/files/list?root=timelapse"

        try:
            async with asyncio.timeout(timeout):
                async with session.get(url, headers=self._headers) as response:
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        raise RuntimeError(
                            f"Timelapse list failed with status {response.status}: {detail}"
                        )
                    data = await response.json()
                    # Response format: {"result": [{"path": "file.mp4", "modified": 1234567890.0, "size": 12345}, ...]}
                    return data.get("result", [])
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Timelapse list timed out after %.1fs",
                timeout,
            )
            raise

    async def fetch_timelapse_file(
        self,
        filename: str,
        timeout: float = 120.0,
    ) -> Optional[bytes]:
        """Fetch timelapse video or preview image from Moonraker file server.

        moonraker-timelapse registers the output directory as 'timelapse' root
        in file_manager. Files are served at /server/files/timelapse/{filename}.

        Args:
            filename: Timelapse filename (e.g., "timelapse_xxx.mp4" or "timelapse_xxx.jpg")
            timeout: Request timeout in seconds (default: 120.0 for large videos)

        Returns:
            Raw file bytes if successful, None if file not found.

        Raises:
            asyncio.TimeoutError: If request exceeds timeout.
            RuntimeError: If fetch fails with non-404 error.
        """
        session = await self._ensure_session()

        from urllib.parse import quote
        encoded_filename = quote(filename, safe="")
        url = f"{self._base_url}/server/files/timelapse/{encoded_filename}"

        try:
            async with asyncio.timeout(timeout):
                async with session.get(url, headers=self._headers) as response:
                    if response.status == 404:
                        LOGGER.debug("Timelapse file not found: %s (url=%s)", filename, url)
                        return None
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        raise RuntimeError(
                            f"Timelapse fetch failed with status {response.status}: {detail}"
                        )
                    return await response.read()
        except asyncio.TimeoutError:
            LOGGER.warning(
                "Timelapse fetch timed out after %.1fs (filename=%s)",
                timeout,
                filename,
            )
            raise

    async def upload_gcode_file(
        self,
        filename: str,
        file_path: str,
        timeout: float = 120.0,
    ) -> str:
        """Upload a GCode file to Moonraker via POST /server/files/upload.

        Uses multipart/form-data upload. Moonraker stores the file in the
        gcodes/ virtual SD card directory.

        Args:
            filename: The target filename (e.g., "benchy.gcode").
            file_path: Local filesystem path to the file to upload.
            timeout: Upload timeout in seconds.

        Returns:
            The stored filename as reported by Moonraker.

        Raises:
            ValueError: If filename is empty.
            RuntimeError: If the upload fails.
        """
        if not filename or not filename.strip():
            raise ValueError("Filename cannot be empty")

        session = await self._ensure_session()
        url = f"{self._base_url}/server/files/upload"

        import aiohttp as _aiohttp

        # quote_fields=False prevents aiohttp from percent-encoding non-ASCII
        # characters in the Content-Disposition filename. Moonraker (Tornado)
        # expects raw UTF-8 filenames, not RFC 5987 encoded ones.
        data = _aiohttp.FormData(quote_fields=False)
        data.add_field(
            "file",
            open(file_path, "rb"),  # noqa: SIM115
            filename=filename.strip(),
            content_type="application/octet-stream",
        )
        data.add_field("root", "gcodes")

        try:
            async with asyncio.timeout(timeout):
                async with session.post(
                    url, data=data, headers=self._headers
                ) as response:
                    if response.status >= 400:
                        detail = self._redact((await response.text()).strip())
                        raise RuntimeError(
                            f"GCode upload failed with status {response.status}: {detail}"
                        )
                    result = await response.json()
                    item = result.get("result", {}).get("item", {})
                    return item.get("path", filename.strip())
        except asyncio.TimeoutError:
            LOGGER.warning(
                "GCode upload timed out after %.1fs (filename=%s)",
                timeout,
                filename,
            )
            raise

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------
    async def _ensure_session(self) -> aiohttp.ClientSession:
        if self._session is None:
            timeout = aiohttp.ClientTimeout(total=None)
            self._session = aiohttp.ClientSession(timeout=timeout)
            self._owns_session = True
        return self._session

    async def _listen_loop(self) -> None:
        backoff = self.reconnect_initial

        while not self._stop_event.is_set():
            try:
                session = await self._ensure_session()
                ws_url = _build_ws_url(self._base_url)
                async with session.ws_connect(ws_url, headers=self._headers) as ws:
                    LOGGER.info("Connected to Moonraker websocket at %s", ws_url)
                    backoff = self.reconnect_initial
                    self._note_ws_connected()

                    self._active_ws = ws
                    try:
                        await self._send_subscription(ws)
                        async for message in ws:
                            if self._stop_event.is_set():
                                break
                            if message.type == aiohttp.WSMsgType.TEXT:
                                await self._dispatch(message.data)
                            elif message.type == aiohttp.WSMsgType.BINARY:
                                pass  # Ignore binary messages
                            elif message.type == aiohttp.WSMsgType.ERROR:
                                raise ws.exception() or RuntimeError("Websocket error")
                    finally:
                        self._active_ws = None
            except asyncio.CancelledError:
                raise
            except Exception as exc:  # pragma: no cover - defensive net handling
                if self._stop_event.is_set():
                    break
                self._note_ws_failure(exc)
                # Apply full jitter to reconnect delay to avoid herd storms. Sleep a random
                # duration uniformly between 0 and the current backoff value, then increase
                # the backoff (capped by reconnect_max) for the next attempt.
                jittered = random.uniform(0, backoff)
                await asyncio.sleep(jittered)
                backoff = min(backoff * 2, self.reconnect_max)

    def _note_ws_connected(self) -> None:
        """Record a successful websocket connection and clear outage state."""

        was_disconnected = (
            self._consecutive_failures > 0 or self._outage_started_at is not None
        )
        if was_disconnected:
            elapsed = (
                time.monotonic() - self._outage_started_at
                if self._outage_started_at is not None
                else 0.0
            )
            LOGGER.info(
                "Moonraker websocket recovered after %d failed attempt(s) (%.0fs outage)",
                self._consecutive_failures,
                elapsed,
            )
        self._ws_connected = True
        self._consecutive_failures = 0
        self._outage_started_at = None
        self._outage_alerted = False

    def _note_ws_failure(self, exc: BaseException) -> None:
        """Record a websocket failure and escalate to CRITICAL on a sustained outage."""

        self._ws_connected = False
        self._consecutive_failures += 1
        if self._outage_started_at is None:
            self._outage_started_at = time.monotonic()

        if (
            self._consecutive_failures >= self.outage_alert_after
            and not self._outage_alerted
        ):
            elapsed = time.monotonic() - self._outage_started_at
            LOGGER.critical(
                "Moonraker websocket unreachable after %d consecutive failures "
                "(%.0fs outage); continuing to retry. Last error: %s",
                self._consecutive_failures,
                elapsed,
                exc,
            )
            self._outage_alerted = True
        else:
            LOGGER.warning(
                "Moonraker websocket error (attempt %d): %s",
                self._consecutive_failures,
                exc,
            )


    async def resubscribe(self) -> None:
        """Re-send printer.objects.subscribe for the active websocket."""

        if self._subscription_objects is None:
            return

        ws = self._active_ws
        if ws is None or ws.closed:
            return

        try:
            await self._send_subscription(ws)
        except asyncio.CancelledError:
            raise
        except Exception:  # pragma: no cover - defensive logging
            LOGGER.exception("Failed to resend Moonraker subscription request")

    async def _dispatch(self, raw_data: str) -> None:
        try:
            payload = json.loads(raw_data)
        except json.JSONDecodeError:
            return  # Discard non-JSON messages

        for callback in list(self._callbacks):
            try:
                result = callback(payload)
                if asyncio.iscoroutine(result):
                    # Bound each async callback so one slow/stuck handler can't
                    # block the websocket message pump and stall all telemetry.
                    await asyncio.wait_for(result, timeout=self._dispatch_timeout)
            except asyncio.TimeoutError:
                LOGGER.error(
                    "Moonraker callback timed out after %.1fs; skipping",
                    self._dispatch_timeout,
                )
            except asyncio.CancelledError:
                raise
            except Exception:  # pragma: no cover - defensive logging
                LOGGER.exception("Moonraker callback failed")

    async def _next_rpc_id(self) -> int:
        """Allocate a unique JSON-RPC request id.

        Guarded by an ``asyncio.Lock`` so concurrent callers on the event loop
        cannot observe or assign a duplicate id.
        """
        async with self._rpc_id_lock:
            self._rpc_id += 1
            return self._rpc_id

    async def _send_subscription(self, ws: aiohttp.ClientWebSocketResponse) -> None:
        # Subscribe to printer status objects. Recovery after a reconnect reuses
        # the last-known subscription set persisted on this adapter instance.
        if not self._subscription_objects:
            LOGGER.warning(
                "No Moonraker subscription objects configured; telemetry will not "
                "flow until set_subscription_objects() is called"
            )
            return

        rpc_id = await self._next_rpc_id()
        payload = {
            "jsonrpc": "2.0",
            "method": "printer.objects.subscribe",
            "id": rpc_id,
            "params": {"objects": self._subscription_objects},
        }

        try:
            await ws.send_json(payload)
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            # Fail-fast: a websocket that cannot accept our subscription will
            # never deliver status updates. Propagate so _listen_loop tears the
            # connection down and reconnects instead of sitting on a silently
            # dead subscription.
            LOGGER.error(
                "Failed to send Moonraker printer objects subscription: %s", exc
            )
            raise


def _build_ws_url(http_url: str) -> str:
    parsed = urlparse(http_url)
    scheme = "ws"
    if parsed.scheme == "https":
        scheme = "wss"

    path = parsed.path.rstrip("/") + "/websocket"
    return urlunparse((scheme, parsed.netloc, path, "", "", ""))
