"""Best-effort host and network signal sampling for the health channel (ADR-0045).

These samplers feed Layer-3 diagnostics and the attribution inputs for the
cloud-side connection-quality verdict:

* **Wi-Fi RSSI** lets the cloud light segment ③ (Agent↔LocalNetwork) and attribute
  instability to a weak local signal ("move closer to the router") instead of
  blaming the platform. Wired hosts have no ``wlan0`` — RSSI is then ``None`` and
  the UI shows "Wired" rather than a false "bad" reading.
* **Host throttling / under-voltage** (Raspberry Pi ``vcgencmd get_throttled``) is one
  of the most common real-world instability root causes; surfacing it turns a
  mysterious flapping printer into an actionable "check your power supply".

Every probe is wrapped so a missing tool, a non-Pi host, or a permission error
degrades to ``None`` and never disturbs the periodic health-publish loop. Probes
that shell out use a short timeout so they cannot stall the event loop.
"""

from __future__ import annotations

import logging
import os
import re
import subprocess
from dataclasses import dataclass
from typing import Optional

LOGGER = logging.getLogger(__name__)

_PROC_NET_WIRELESS = "/proc/net/wireless"
_SYS_CLASS_NET = "/sys/class/net"
_THERMAL_ZONE0 = "/sys/class/thermal/thermal_zone0/temp"
_SUBPROCESS_TIMEOUT_SECONDS = 0.5

# Uplink classification reported on the health channel so the cloud can distinguish a genuinely
# wired host (healthy local link, no RSSI) from one whose signal we simply cannot read (e.g. a
# non-Linux dev host) — the latter must NOT be mislabelled "Wired" (ADR-0045).
LINK_TYPE_WIFI = "wifi"
LINK_TYPE_WIRED = "wired"
LINK_TYPE_UNKNOWN = "unknown"

# Raspberry Pi `vcgencmd get_throttled` bit meanings we care about. Bit 0 = currently
# under-voltage; bit 16 = under-voltage has occurred since boot.
_UNDERVOLTAGE_NOW_BIT = 0x1
_UNDERVOLTAGE_OCCURRED_BIT = 0x10000


@dataclass(frozen=True)
class NetworkSample:
    """Local-network signal. All fields ``None`` on a wired/unknown host."""

    wifi_rssi_dbm: Optional[float] = None
    interface: Optional[str] = None
    link_type: Optional[str] = None
    """One of ``LINK_TYPE_WIFI``/``LINK_TYPE_WIRED``/``LINK_TYPE_UNKNOWN`` (``None`` only on error)."""


@dataclass(frozen=True)
class HostSample:
    """Host hardware health. All fields ``None`` when the host cannot report them."""

    throttled: Optional[bool] = None
    throttled_flags: Optional[str] = None
    under_voltage: Optional[bool] = None
    temp_c: Optional[float] = None


def sample_network() -> NetworkSample:
    """Classify the agent host's uplink and read Wi-Fi RSSI when applicable. Best-effort.

    Three outcomes (ADR-0045):

    * **wifi** — ``/proc/net/wireless`` reports an associated interface with a signal level. The
      RSSI feeds segment ③ and "weak Wi-Fi" attribution.
    * **wired** — no Wi-Fi signal, but ``/sys/class/net`` shows an active non-wireless interface.
      A healthy wired link has no RSSI; the cloud renders it green ("Wired"), not a false "bad".
    * **unknown** — neither could be determined (e.g. a non-Linux dev host). The cloud renders
      this as "Unknown" — crucially **not** "Wired" — so a Wi-Fi printer whose signal we cannot
      read is never mislabelled.
    """
    wifi = _read_wifi_signal()
    if wifi is not None:
        rssi, interface = wifi
        return NetworkSample(wifi_rssi_dbm=rssi, interface=interface, link_type=LINK_TYPE_WIFI)

    if _has_active_wired_link():
        return NetworkSample(link_type=LINK_TYPE_WIRED)

    return NetworkSample(link_type=LINK_TYPE_UNKNOWN)


def _read_wifi_signal() -> Optional[tuple[float, str]]:
    """Read (RSSI dBm, interface) from ``/proc/net/wireless``, or ``None`` if no associated Wi-Fi."""
    try:
        with open(_PROC_NET_WIRELESS, "r", encoding="utf-8") as handle:
            lines = handle.readlines()
    except OSError:
        # No wireless stack (wired host / non-Linux) — fall through to wired/unknown detection.
        return None

    # The first two lines are headers; data rows look like:
    #   wlan0: 0000   54.  -56.  -256        0 ...
    # where the 4th whitespace token (index 3) is the signal level in dBm.
    for line in lines[2:]:
        parts = line.split()
        if len(parts) < 4 or not parts[0].endswith(":"):
            continue
        interface = parts[0].rstrip(":")
        try:
            rssi = float(parts[3].rstrip("."))
        except ValueError:
            continue
        return (rssi, interface)

    return None


def _has_active_wired_link() -> bool:
    """True when ``/sys/class/net`` shows an up, non-loopback, non-wireless interface (Linux only)."""
    try:
        names = os.listdir(_SYS_CLASS_NET)
    except OSError:
        return False

    for name in names:
        if name == "lo":
            continue

        base = os.path.join(_SYS_CLASS_NET, name)
        # Wireless interfaces are classified via _read_wifi_signal(); skip them here.
        if os.path.isdir(os.path.join(base, "wireless")):
            continue

        try:
            with open(os.path.join(base, "operstate"), "r", encoding="utf-8") as handle:
                if handle.read().strip() != "up":
                    continue
        except OSError:
            continue

        return True

    return False


def sample_host() -> HostSample:
    """Sample Raspberry-Pi-style throttling/under-voltage and CPU temperature."""
    throttled, throttled_flags, under_voltage = _sample_throttled()
    temp_c = _sample_temperature_c()
    return HostSample(
        throttled=throttled,
        throttled_flags=throttled_flags,
        under_voltage=under_voltage,
        temp_c=temp_c,
    )


def _sample_throttled() -> tuple[Optional[bool], Optional[str], Optional[bool]]:
    raw = _run(["vcgencmd", "get_throttled"])
    if raw is None:
        return (None, None, None)

    match = re.search(r"throttled=(0x[0-9a-fA-F]+)", raw)
    if match is None:
        return (None, None, None)

    flags = match.group(1)
    try:
        value = int(flags, 16)
    except ValueError:
        return (None, flags, None)

    under_voltage = bool(value & (_UNDERVOLTAGE_NOW_BIT | _UNDERVOLTAGE_OCCURRED_BIT))
    return (value != 0, flags, under_voltage)


def _sample_temperature_c() -> Optional[float]:
    # Prefer the sysfs thermal zone (cheap file read, no subprocess).
    try:
        with open(_THERMAL_ZONE0, "r", encoding="utf-8") as handle:
            milli = int(handle.read().strip())
        return round(milli / 1000.0, 1)
    except (OSError, ValueError):
        pass

    raw = _run(["vcgencmd", "measure_temp"])
    if raw is None:
        return None
    match = re.search(r"temp=([0-9.]+)", raw)
    if match is None:
        return None
    try:
        return round(float(match.group(1)), 1)
    except ValueError:
        return None


def _run(command: list[str]) -> Optional[str]:
    """Run *command* with a short timeout, returning stdout or ``None`` on any error."""
    try:
        completed = subprocess.run(  # noqa: S603 - fixed, non-shell command list
            command,
            capture_output=True,
            text=True,
            timeout=_SUBPROCESS_TIMEOUT_SECONDS,
            check=False,
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if completed.returncode != 0:
        return None
    return completed.stdout
