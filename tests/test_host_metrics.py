"""Tests for best-effort host/network metric sampling (ADR-0045)."""

from __future__ import annotations

import subprocess

from moonraker_owl import host_metrics
from moonraker_owl.host_metrics import HostSample, NetworkSample

_PROC_NET_WIRELESS_SAMPLE = """\
Inter-| sta-|   Quality        |   Discarded packets               | Missed | WE
 face | tus | link level noise |  nwid  crypt   frag  retry   misc | beacon | 22
 wlan0: 0000   54.  -58.  -256        0      0      0      0      0        0
"""


def test_sample_network_parses_rssi(monkeypatch) -> None:
    real_open = open

    def fake_open(path, *args, **kwargs):
        if path == host_metrics._PROC_NET_WIRELESS:
            from io import StringIO

            return StringIO(_PROC_NET_WIRELESS_SAMPLE)
        return real_open(path, *args, **kwargs)

    monkeypatch.setattr("moonraker_owl.host_metrics.open", fake_open, raising=False)

    sample = host_metrics.sample_network()
    assert sample.interface == "wlan0"
    assert sample.wifi_rssi_dbm == -58.0
    assert sample.link_type == host_metrics.LINK_TYPE_WIFI


def test_sample_network_wired_when_active_ethernet(monkeypatch) -> None:
    # No Wi-Fi signal, but an active wired interface ⇒ "wired" (healthy link, no RSSI).
    def fake_open(path, *args, **kwargs):
        raise OSError("no such file")

    monkeypatch.setattr("moonraker_owl.host_metrics.open", fake_open, raising=False)
    monkeypatch.setattr(host_metrics, "_has_active_wired_link", lambda: True)

    sample = host_metrics.sample_network()
    assert sample.wifi_rssi_dbm is None
    assert sample.interface is None
    assert sample.link_type == host_metrics.LINK_TYPE_WIRED


def test_sample_network_unknown_when_undeterminable(monkeypatch) -> None:
    # Neither Wi-Fi nor a wired link could be determined (e.g. a non-Linux dev host) ⇒ "unknown".
    # Crucially NOT "wired": a Wi-Fi printer whose signal we cannot read must never read "Wired".
    def fake_open(path, *args, **kwargs):
        raise OSError("no such file")

    monkeypatch.setattr("moonraker_owl.host_metrics.open", fake_open, raising=False)
    monkeypatch.setattr(host_metrics, "_has_active_wired_link", lambda: False)

    sample = host_metrics.sample_network()
    assert sample.wifi_rssi_dbm is None
    assert sample.interface is None
    assert sample.link_type == host_metrics.LINK_TYPE_UNKNOWN


def test_has_active_wired_link_detects_up_non_wireless_interface(monkeypatch) -> None:
    import os
    from io import StringIO

    monkeypatch.setattr(os, "listdir", lambda path: ["lo", "wlan0", "eth0"])
    # Only wlan0 is wireless (has a /wireless subdir) and must be skipped.
    monkeypatch.setattr(
        os.path, "isdir", lambda p: str(p).endswith(os.path.join("wlan0", "wireless"))
    )

    def fake_open(path, *args, **kwargs):
        if str(path).endswith(os.path.join("eth0", "operstate")):
            return StringIO("up\n")
        raise OSError("no such file")

    monkeypatch.setattr("moonraker_owl.host_metrics.open", fake_open, raising=False)

    assert host_metrics._has_active_wired_link() is True


def test_sample_host_parses_throttled_and_temp(monkeypatch) -> None:
    def fake_run(command):
        if command[:2] == ["vcgencmd", "get_throttled"]:
            return "throttled=0x50005\n"
        return None

    monkeypatch.setattr(host_metrics, "_run", fake_run)
    monkeypatch.setattr(host_metrics, "_sample_temperature_c", lambda: 61.3)

    sample = host_metrics.sample_host()
    assert sample.throttled is True
    assert sample.throttled_flags == "0x50005"
    # 0x50005 has bit 0 (under-voltage now) and bit 16 (under-voltage occurred) set.
    assert sample.under_voltage is True
    assert sample.temp_c == 61.3


def test_sample_host_healthy(monkeypatch) -> None:
    monkeypatch.setattr(host_metrics, "_run", lambda command: "throttled=0x0\n")
    monkeypatch.setattr(host_metrics, "_sample_temperature_c", lambda: 42.0)

    sample = host_metrics.sample_host()
    assert sample.throttled is False
    assert sample.under_voltage is False


def test_sample_host_unavailable_returns_none(monkeypatch) -> None:
    monkeypatch.setattr(host_metrics, "_run", lambda command: None)
    monkeypatch.setattr(host_metrics, "_sample_temperature_c", lambda: None)

    sample = host_metrics.sample_host()
    assert sample == HostSample()


def test_run_swallows_missing_binary(monkeypatch) -> None:
    def boom(*args, **kwargs):
        raise FileNotFoundError("vcgencmd not found")

    monkeypatch.setattr(subprocess, "run", boom)
    assert host_metrics._run(["vcgencmd", "get_throttled"]) is None


def test_sample_temperature_prefers_sysfs_milli_degrees(monkeypatch) -> None:
    # The sysfs thermal zone reports integer milli-degrees; convert to °C and prefer it over the
    # vcgencmd fallback (which here would report a different value, proving sysfs wins).
    from io import StringIO

    monkeypatch.setattr(
        "moonraker_owl.host_metrics.open",
        lambda *args, **kwargs: StringIO("47200\n"),
        raising=False,
    )
    monkeypatch.setattr(host_metrics, "_run", lambda command: "temp=99.9'C\n")

    assert host_metrics._sample_temperature_c() == 47.2


def test_sample_temperature_falls_back_to_vcgencmd(monkeypatch) -> None:
    # sysfs unreadable ⇒ fall back to `vcgencmd measure_temp` and parse its temp=NN.N'C output.
    def fake_open(*args, **kwargs):
        raise OSError("no thermal zone")

    monkeypatch.setattr("moonraker_owl.host_metrics.open", fake_open, raising=False)
    monkeypatch.setattr(host_metrics, "_run", lambda command: "temp=55.3'C\n")

    assert host_metrics._sample_temperature_c() == 55.3


def test_sample_temperature_none_when_both_sources_fail(monkeypatch) -> None:
    # Neither sysfs nor vcgencmd available ⇒ None, so the health-publish loop is never disturbed.
    def fake_open(*args, **kwargs):
        raise OSError("no thermal zone")

    monkeypatch.setattr("moonraker_owl.host_metrics.open", fake_open, raising=False)
    monkeypatch.setattr(host_metrics, "_run", lambda command: None)

    assert host_metrics._sample_temperature_c() is None
