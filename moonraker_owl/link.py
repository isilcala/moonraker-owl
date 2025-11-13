"""Device linking workflow."""

from __future__ import annotations

import asyncio
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import Optional

import aiohttp

from .config import OwlConfig, save_config
from .constants import DEFAULT_CREDENTIALS_PATH

LOGGER = logging.getLogger(__name__)

DEFAULT_TIMEOUT_SECONDS = 120.0
DEFAULT_POLL_INTERVAL_SECONDS = 5.0


class DeviceLinkingError(RuntimeError):
    """Raised when the linking flow fails."""


@dataclass(slots=True)
class DeviceCredentials:
    tenant_id: str
    printer_id: str
    device_id: str
    device_private_key: str  # Base64-encoded Ed25519 private key (32 bytes)
    linked_at: str


async def link_device(
    base_url: str,
    link_code: str,
    *,
    session: Optional[aiohttp.ClientSession] = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DeviceCredentials:
    """Request credentials from Owl Cloud, polling until accepted or timeout."""

    if not link_code:
        raise DeviceLinkingError("Link code cannot be empty")

    url = base_url.rstrip("/") + "/api/v1/devices/link"
    loop = asyncio.get_running_loop()
    deadline = loop.time() + timeout

    owns_session = session is None
    if session is None:
        client_timeout = aiohttp.ClientTimeout(total=timeout + 30)
        session = aiohttp.ClientSession(timeout=client_timeout)

    try:
        assert session is not None  # narrow type for linters
        while True:
            async with session.post(url, json={"linkCode": link_code}) as response:
                if response.status == 200:
                    payload = await response.json()
                    try:
                        printer_id = str(payload["printerId"])
                        device_id = str(payload["deviceId"])
                        device_private_key = str(payload["devicePrivateKey"])
                    except KeyError as exc:
                        raise DeviceLinkingError(
                            f"Response missing expected field: {exc.args[0]}"
                        ) from exc

                    tenant_id = str(payload.get("tenantId", ""))

                    return DeviceCredentials(
                        tenant_id=tenant_id,
                        printer_id=printer_id,
                        device_id=device_id,
                        device_private_key=device_private_key,
                        linked_at=str(payload.get("linkedAt", "")),
                    )

                if response.status == 404:
                    if loop.time() >= deadline:
                        raise DeviceLinkingError(
                            "Linking timed out waiting for acceptance"
                        )
                    await asyncio.sleep(poll_interval)
                    continue

                detail = await response.text()
                raise DeviceLinkingError(
                    f"Unexpected response {response.status} from Owl Cloud: {detail.strip()}"
                )
    finally:
        if owns_session and session is not None:
            await session.close()


def perform_linking(
    config: OwlConfig,
    *,
    force: bool = False,
    link_code: Optional[str] = None,
    credentials_path: Optional[Path] = None,
    poll_interval: float = DEFAULT_POLL_INTERVAL_SECONDS,
    timeout: float = DEFAULT_TIMEOUT_SECONDS,
) -> DeviceCredentials:
    """Run the interactive linking workflow and persist credentials."""

    target_path = _resolve_credentials_path(credentials_path)

    if target_path.exists() and not force:
        raise DeviceLinkingError(
            f"Credentials already exist at {target_path}. Use --force to re-link."
        )

    code = link_code or input("Enter printer link code: ").strip()
    if not code:
        raise DeviceLinkingError("Link code cannot be empty")

    LOGGER.info("Linking printer via Owl Cloud at %s", config.cloud.base_url)

    try:
        credentials = asyncio.run(
            link_device(
                config.cloud.base_url,
                code,
                poll_interval=poll_interval,
                timeout=timeout,
            )
        )
    except DeviceLinkingError:
        raise
    except Exception as exc:  # pragma: no cover - defensive
        raise DeviceLinkingError(f"Linking failed unexpectedly: {exc}") from exc

    _persist_credentials(target_path, credentials)
    _update_config_with_credentials(config, credentials)
    save_config(config)

    LOGGER.info(
        "Linked printer %s (device %s) to tenant %s",
        credentials.printer_id,
        credentials.device_id,
        credentials.tenant_id,
    )

    return credentials


def _resolve_credentials_path(path: Optional[Path]) -> Path:
    credentials_path = path or DEFAULT_CREDENTIALS_PATH
    credentials_path.parent.mkdir(parents=True, exist_ok=True)
    return credentials_path


def _persist_credentials(path: Path, credentials: DeviceCredentials) -> None:
    import os
    
    payload = {
        "printerId": credentials.printer_id,
        "deviceId": credentials.device_id,
        "devicePrivateKey": credentials.device_private_key,
        "linkedAt": credentials.linked_at,
    }

    if credentials.tenant_id:
        payload["tenantId"] = credentials.tenant_id

    with path.open("w", encoding="utf-8") as stream:
        json.dump(payload, stream, indent=2)
    
    # Secure file permissions (Unix only)
    if hasattr(os, "chmod"):
        os.chmod(path, 0o600)  # rw-------
        LOGGER.info("Set credentials file permissions to 0600")


def _update_config_with_credentials(
    config: OwlConfig, credentials: DeviceCredentials
) -> None:
    tenant_id = credentials.tenant_id
    if tenant_id:
        broker_username = f"{tenant_id}:{credentials.device_id}"
    else:
        broker_username = credentials.device_id

    config.cloud.username = broker_username
    # Note: password field no longer used - JWT authentication only

    if not config.raw.has_section("cloud"):
        config.raw.add_section("cloud")

    config.raw.set("cloud", "username", broker_username)
    config.raw.set("cloud", "device_id", credentials.device_id)
    config.raw.set("cloud", "printer_id", credentials.printer_id)
    config.raw.set("cloud", "device_private_key", credentials.device_private_key)

    if tenant_id:
        config.raw.set("cloud", "tenant_id", tenant_id)
    elif config.raw.has_option("cloud", "tenant_id"):
        config.raw.remove_option("cloud", "tenant_id")
