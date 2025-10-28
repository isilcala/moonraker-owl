"""Selectors that compose channel payloads for the telemetry pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional

from .state_engine import PrinterContext, PrinterStateEngine
from .state_store import MoonrakerStateStore
from .trackers import HeaterMonitor, SessionInfo


class OverviewSelector:
    def __init__(self, *, heartbeat_seconds: int = 60) -> None:
        self._heartbeat_seconds = heartbeat_seconds
        self._state_engine = PrinterStateEngine()

    def build(
        self,
        store: MoonrakerStateStore,
        session: SessionInfo,
        heater_monitor: HeaterMonitor,
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        print_stats_snapshot = store.get("print_stats")
        state = (
            print_stats_snapshot.data.get("state")
            if print_stats_snapshot is not None
            else None
        )

        context = PrinterContext(
            observed_at=observed_at,
            has_active_job=session.has_active_job,
            is_heating=heater_monitor.is_heating_for_print(),
        )

        overview: Dict[str, Any] = {
            "printerStatus": self._state_engine.resolve(state, context)
        }

        if session.message:
            overview["subStatus"] = session.message

        job_payload = _build_job_payload(session)
        if job_payload:
            overview["job"] = job_payload

        if session.progress_percent is not None:
            overview["elapsedSeconds"] = session.elapsed_seconds or 0
            overview["estimatedTimeRemainingSeconds"] = session.remaining_seconds or 0

        overview["flags"] = {
            "isHeating": heater_monitor.is_heating_for_print(),
            "hasActiveJob": session.has_active_job,
            "watchWindowActive": False,
        }

        overview["heartbeatSeconds"] = self._heartbeat_seconds
        overview["lastUpdatedUtc"] = observed_at.replace(microsecond=0).isoformat()

        return overview


class TelemetrySelector:
    def __init__(self) -> None:
        pass

    def build(
        self,
        store: MoonrakerStateStore,
        *,
        mode: str,
        max_hz: float,
        watch_window_expires: Optional[datetime],
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        sensors = _collect_sensors(store)
        cadence: Dict[str, Any] = {
            "mode": mode,
            "maxHz": max_hz,
        }
        if watch_window_expires is not None:
            cadence["watchWindowExpiresUtc"] = watch_window_expires.replace(
                microsecond=0
            ).isoformat()

        return {
            "cadence": cadence,
            "sensors": sensors,
        }


class EventsSelector:
    def build(
        self,
        *,
        events: List[Dict[str, Any]],
        observed_at: datetime,
    ) -> Optional[Dict[str, Any]]:
        if not events:
            return None

        return {
            "cadence": {
                "maxPerSecond": 1,
                "maxPerMinute": 20,
            },
            "items": events,
        }


def _build_job_payload(session: SessionInfo) -> Optional[Dict[str, Any]]:
    if not session.session_id:
        return None

    payload: Dict[str, Any] = {
        "sessionId": session.session_id,
        "jobId": session.job_id or session.session_id,
    }

    if session.job_name:
        payload["name"] = session.job_name
        payload.setdefault("sourcePath", session.source_path or session.job_name)

    if session.progress_percent is not None:
        payload["progressPercent"] = round(session.progress_percent, 3)

    layers = {}
    if session.layer_current is not None:
        layers["current"] = session.layer_current
    if session.layer_total is not None:
        layers["total"] = session.layer_total
    if layers:
        payload["layers"] = layers

    payload["progress"] = {
        "elapsedSeconds": session.elapsed_seconds or 0,
        "estimatedTimeRemainingSeconds": session.remaining_seconds or 0,
    }

    if session.message:
        payload["message"] = session.message

    return payload


def _collect_sensors(store: MoonrakerStateStore) -> List[Dict[str, Any]]:
    sensors: List[Dict[str, Any]] = []

    for section in store.iter_sections():
        channel = _normalise_channel_name(section.name)
        if channel is None:
            continue

        sensor_type = _infer_sensor_type(section.name)
        unit = _infer_unit(section.name)
        value = _round_float(
            section.data.get("temperature") or section.data.get("value")
        )
        target = _round_float(section.data.get("target"))
        status = section.data.get("state") or "ok"

        sensors.append(
            {
                "channel": channel,
                "type": sensor_type,
                "unit": unit,
                "value": value,
                **({"target": target} if target is not None else {}),
                "status": status,
                "lastUpdatedUtc": section.observed_at.replace(
                    microsecond=0
                ).isoformat(),
            }
        )

    sensors.sort(key=lambda entry: entry["channel"])
    return sensors


def _normalise_channel_name(name: str) -> Optional[str]:
    if name.startswith("extruder"):
        suffix = name[len("extruder") :]
        return "extruder" + suffix.capitalize() if suffix else "extruder"
    if name == "heater_bed":
        return "heaterBed"
    if name.startswith("heater_generic"):
        return name.replace("heater_generic ", "heaterGeneric-")
    if name in {"temperatures", "toolhead"}:
        return None
    if name in {"temperature_fan", "temperature_fans", "fan", "part_fan"}:
        return "partCoolingFan"
    return None


def _infer_sensor_type(name: str) -> str:
    if name.startswith("extruder") or name.startswith("heater"):
        return "heater"
    return "fan"


def _infer_unit(name: str) -> str:
    if name.startswith("extruder") or name.startswith("heater"):
        return "celsius"
    return "percent"


def _round_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    try:
        return round(float(value), 1)
    except (TypeError, ValueError):
        return None
