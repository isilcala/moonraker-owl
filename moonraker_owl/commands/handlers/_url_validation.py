"""URL allowlist + content-type guards for cloud-supplied URLs.

Used by the Cold Path task handlers (`task:download-gcode`,
`task:capture-image`) to defend against a compromised or misbehaving
cloud control plane (audit A-02, A-03). The agent must not blindly
follow whatever URL the cloud sends:

* SSRF — `http://192.168.1.1/...`, `http://127.0.0.1:7125/...`,
  `http://[::1]/...`, `http://internal-svc.local/...` should be rejected.
* Cross-origin handoff — uploads / downloads should only target hosts
  the operator explicitly trusts.
* Disk-fill / OOM — downloads should be hard-capped regardless of any
  cloud-supplied size hint.
* Content-type confusion — `task:download-gcode` should only accept
  responses that look like text/binary GCode, never HTML/JSON.

Allowlist semantics:
    * The host of ``cloud.base_url`` is always implicitly trusted.
    * ``cloud.allowed_storage_hosts`` extends the allowlist:
        - ``"files.example.com"`` matches that exact host.
        - ``".amazonaws.com"`` matches any subdomain (suffix match).
        - ``"127.0.0.1"`` / ``"localhost"`` allow loopback (dev/MinIO).
    * IP-literal hosts in private/loopback/link-local ranges are
      rejected *unless* the literal appears verbatim in the allowlist
      — opt-in for dev environments.

This module contains no I/O. URL host parsing is purely lexical;
no DNS resolution is performed (resolution-time SSRF is a separate
concern handled by deploy-time network policy).
"""
from __future__ import annotations

import ipaddress
from typing import Iterable, Optional
from urllib.parse import urlparse

from ..types import CommandProcessingError

# A 256 MiB hard cap on cloud-supplied GCode downloads. Anything bigger
# is almost certainly an attempt to fill /tmp on the Pi. The largest
# real-world GCode files we have observed are ~80 MiB.
MAX_DOWNLOAD_BYTES: int = 256 * 1024 * 1024

# A 10 MiB hard cap on captured-image uploads. The cloud is allowed to
# pass a smaller `maxFileSizeBytes`, but never a larger one. A Pi 4
# under Klipper has very little RAM headroom; reading more than this
# into memory (`response.read()` style) risks OOM-killing the agent.
MAX_CAPTURE_UPLOAD_BYTES: int = 10 * 1024 * 1024

# Permitted Content-Type values for `task:download-gcode`. Compared
# case-insensitively after stripping `;` parameters.
ALLOWED_GCODE_CONTENT_TYPES: frozenset[str] = frozenset(
    {
        "text/plain",
        "application/octet-stream",
        "application/x-gcode",
        "application/gcode",
    }
)


def _parse_host(url: str) -> Optional[str]:
    """Return the lower-case hostname for *url* or ``None`` if missing."""
    try:
        parsed = urlparse(url)
    except (ValueError, TypeError):
        return None
    if not parsed.hostname:
        return None
    return parsed.hostname.lower()


def _is_private_ip_literal(host: str) -> bool:
    """True if *host* is an IP literal that resolves to a non-public range."""
    try:
        ip = ipaddress.ip_address(host)
    except ValueError:
        return False
    return (
        ip.is_private
        or ip.is_loopback
        or ip.is_link_local
        or ip.is_multicast
        or ip.is_reserved
        or ip.is_unspecified
    )


def _host_matches_allow_entry(host: str, entry: str) -> bool:
    """Return True if *host* matches the allowlist *entry*.

    ``entry`` may be:
        * a bare host (``"files.example.com"``) — exact match
        * a suffix starting with a dot (``".amazonaws.com"``) —
          matches any subdomain (and the entry without the leading dot)
    """
    entry = entry.strip().lower()
    if not entry:
        return False
    if entry.startswith("."):
        bare = entry[1:]
        return host == bare or host.endswith(entry)
    return host == entry


def build_allowlist(
    *, cloud_base_url: str, extra_allowed_hosts: Iterable[str]
) -> tuple[str, ...]:
    """Build the effective allowlist for URL validation.

    The host of *cloud_base_url* is always included. Additional hosts
    come from operator config. Entries are de-duplicated.
    """
    seen: list[str] = []
    base_host = _parse_host(cloud_base_url)
    if base_host:
        seen.append(base_host)
    for entry in extra_allowed_hosts:
        if not isinstance(entry, str):
            continue
        normalised = entry.strip().lower()
        if not normalised or normalised in seen:
            continue
        seen.append(normalised)
    return tuple(seen)


def validate_external_url(
    url: str,
    *,
    field: str,
    command_id: str | None,
    allowlist: Iterable[str],
) -> str:
    """Validate that *url* is safe to fetch / upload to.

    Returns the validated host on success.

    Raises ``CommandProcessingError(code="invalid_<field>_url")`` for
    any of: missing/empty value, non-http(s) scheme, missing host,
    private/loopback IP literal not explicitly allowlisted, or host
    not in the allowlist.

    Note: error messages do NOT echo the user-controlled URL back —
    only the host is echoed (or no value at all for malformed input)
    to limit log-injection surface.
    """
    code = f"invalid_{field}_url"
    if not isinstance(url, str) or not url:
        raise CommandProcessingError(
            f"{field} parameter is required and must be a non-empty string",
            code=code,
            command_id=command_id,
        )

    try:
        parsed = urlparse(url)
    except (ValueError, TypeError) as exc:  # pragma: no cover - defensive
        raise CommandProcessingError(
            f"{field} is not a valid URL",
            code=code,
            command_id=command_id,
        ) from exc

    scheme = (parsed.scheme or "").lower()
    if scheme not in {"http", "https"}:
        raise CommandProcessingError(
            f"{field} must use http(s); got scheme={scheme or '<empty>'}",
            code=code,
            command_id=command_id,
        )

    host = (parsed.hostname or "").lower()
    if not host:
        raise CommandProcessingError(
            f"{field} is missing a host",
            code=code,
            command_id=command_id,
        )

    allow_tuple = tuple(e.strip().lower() for e in allowlist if isinstance(e, str))
    matched = any(_host_matches_allow_entry(host, e) for e in allow_tuple if e)

    if _is_private_ip_literal(host) and not matched:
        # Dev (MinIO at 127.0.0.1) must opt-in by listing the literal.
        raise CommandProcessingError(
            f"{field} resolves to a non-public address ({host}) "
            "and is not in cloud.allowed_storage_hosts",
            code=code,
            command_id=command_id,
        )

    if not matched:
        raise CommandProcessingError(
            f"{field} host '{host}' is not in cloud.allowed_storage_hosts "
            "and does not match cloud.base_url",
            code=code,
            command_id=command_id,
        )

    return host


def validate_gcode_content_type(
    content_type: str | None, *, command_id: str | None
) -> None:
    """Reject responses whose Content-Type is not GCode-compatible.

    A missing Content-Type header is permitted (some CDNs strip it);
    HTML / JSON / other application types are rejected so a compromised
    cloud cannot smuggle a wrong-format payload into Moonraker's
    GCode store.
    """
    if not content_type:
        # Permissive on missing — many storage providers omit it.
        return
    primary = content_type.split(";", 1)[0].strip().lower()
    if primary not in ALLOWED_GCODE_CONTENT_TYPES:
        raise CommandProcessingError(
            f"unexpected Content-Type for GCode download: {primary}",
            code="invalid_content_type",
            command_id=command_id,
        )
