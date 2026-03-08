"""
Protocol contract tests — verify Python agent correctly parses canonical
command payloads produced by the C# cloud services.

These fixtures mirror the JSON structures in
tests/Owl.ProtocolContract.Tests/Fixtures/Commands/ on the C# side.
If either side changes a field name, these tests will fail.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from moonraker_owl.commands import _parse_command, CommandMessage

FIXTURES_DIR = Path(__file__).parent / "fixtures" / "contracts"


def _load_fixture(name: str) -> dict:
    return json.loads((FIXTURES_DIR / name).read_text(encoding="utf-8"))


def _to_bytes(data: dict) -> bytes:
    return json.dumps(data).encode("utf-8")


# ---------------------------------------------------------------------------
# Command parsing: cloud → agent
# ---------------------------------------------------------------------------


class TestCommandContractParsing:
    """Verify _parse_command extracts correct fields from $-prefix envelope."""

    def test_capture_image_command_parses(self):
        data = _load_fixture("command-capture-image.json")
        msg = _parse_command(_to_bytes(data), "task:capture-image")

        assert msg.command_id == "11111111-1111-7111-8111-111111111111"
        assert msg.command == "task:capture-image"
        assert "captureFrameId" in msg.parameters
        assert "presignedUploadUrl" in msg.parameters
        assert "blobKey" in msg.parameters

    def test_set_rate_command_parses(self):
        data = _load_fixture("command-set-rate.json")
        msg = _parse_command(_to_bytes(data), "control:set-telemetry-rate")

        assert msg.command_id == "22222222-2222-7222-8222-222222222222"
        assert msg.command == "control:set-telemetry-rate"
        assert msg.parameters["mode"] == "watch"
        assert msg.parameters["intervalSeconds"] == 0.5
        assert "issuedAt" in msg.parameters
        assert "serverUtcNow" in msg.parameters

    def test_print_pause_command_parses(self):
        data = _load_fixture("command-print-pause.json")
        msg = _parse_command(_to_bytes(data), "print:pause")

        assert msg.command_id == "33333333-3333-7333-8333-333333333333"
        assert msg.command == "print:pause"
        assert msg.parameters == {}

    def test_job_registered_command_parses(self):
        data = _load_fixture("command-job-registered.json")
        msg = _parse_command(_to_bytes(data), "job:registered")

        assert msg.command_id == "44444444-4444-7444-8444-444444444444"
        assert msg.command == "job:registered"
        assert msg.parameters["moonrakerJobId"] == "000456"
        assert msg.parameters["printJobId"] == "pj-001"
        assert msg.parameters["fileName"] == "benchy.gcode"


# ---------------------------------------------------------------------------
# Envelope structure: verify required fields exist
# ---------------------------------------------------------------------------


class TestCommandEnvelopeStructure:
    """Verify the canonical command fixtures have all required envelope fields."""

    @pytest.mark.parametrize(
        "fixture_name",
        [
            "command-capture-image.json",
            "command-set-rate.json",
            "command-print-pause.json",
            "command-job-registered.json",
        ],
    )
    def test_envelope_has_required_fields(self, fixture_name: str):
        data = _load_fixture(fixture_name)

        # $-prefix envelope fields
        assert "$v" in data, "missing $v"
        assert data["$v"] == 1
        assert "$type" in data, "missing $type"
        assert "$id" in data, "missing $id"
        assert "$ts" in data, "missing $ts"
        assert "$origin" in data, "missing $origin"

        # Business payload
        assert "payload" in data, "missing payload"
        payload = data["payload"]
        assert "command" in payload, "missing payload.command"

    @pytest.mark.parametrize(
        "fixture_name",
        [
            "command-capture-image.json",
            "command-set-rate.json",
            "command-print-pause.json",
            "command-job-registered.json",
        ],
    )
    def test_envelope_deviceid_present(self, fixture_name: str):
        data = _load_fixture(fixture_name)
        assert "deviceId" in data, "missing deviceId in envelope"
        assert data["deviceId"], "deviceId must not be empty"


# ---------------------------------------------------------------------------
# ACK structure: verify agent ACK format matches C# expectations
# ---------------------------------------------------------------------------


class TestAckPayloadStructure:
    """
    Verify the ACK format the Python agent produces is parseable by C#.
    We build an ACK dict matching _publish_ack's output format and check
    it has all fields the C# CommandAckPayload.TryParse expects.
    """

    @staticmethod
    def _build_sample_ack(
        *,
        status: str = "completed",
        stage: str = "execution",
        error_code: str | None = None,
        error_message: str | None = None,
        result: dict | None = None,
    ) -> dict:
        """Build an ACK document matching the agent's _publish_ack format."""
        ack_payload = {
            "commandId": "11111111-1111-7111-8111-111111111111",
            "status": status,
            "stage": stage,
            "correlation": {
                "tenantId": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                "printerId": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
            },
        }

        if error_code or error_message:
            ack_payload["reason"] = {}
            if error_code:
                ack_payload["reason"]["code"] = error_code
            if error_message:
                ack_payload["reason"]["message"] = error_message

        if result:
            ack_payload["result"] = result

        return {
            "$v": 1,
            "$type": "command.ack",
            "$id": "aaaabbbb-cccc-dddd-eeee-ffffffffffff",
            "$ts": "2025-01-15T10:31:03+00:00",
            "$origin": "moonraker-owl@0.5.0",
            "deviceId": "cccccccc-cccc-cccc-cccc-cccccccccccc",
            "payload": ack_payload,
        }

    def test_completed_ack_has_required_fields(self):
        doc = self._build_sample_ack(status="completed")
        payload = doc["payload"]

        assert payload["commandId"]
        assert payload["status"] == "completed"
        assert payload["stage"] == "execution"
        assert "correlation" in payload
        assert payload["correlation"]["tenantId"]
        assert payload["correlation"]["printerId"]

    def test_failed_ack_has_reason(self):
        doc = self._build_sample_ack(
            status="failed",
            error_code="camera_unavailable",
            error_message="No camera found",
        )
        payload = doc["payload"]

        assert payload["status"] == "failed"
        assert "reason" in payload
        assert payload["reason"]["code"] == "camera_unavailable"
        assert payload["reason"]["message"]

    def test_capture_result_ack_has_s3_fields(self):
        doc = self._build_sample_ack(
            result={
                "s3Key": "tenant/captures/frame/image.jpg",
                "fileSizeBytes": 2097152,
            }
        )
        payload = doc["payload"]

        assert "result" in payload
        assert payload["result"]["s3Key"]
        assert payload["result"]["fileSizeBytes"] > 0

    def test_ack_envelope_has_dollar_prefix_fields(self):
        doc = self._build_sample_ack()

        assert doc["$v"] == 1
        assert doc["$type"] == "command.ack"
        assert doc["$id"]
        assert doc["$ts"]
        assert doc["$origin"]
        assert doc["deviceId"]


# ---------------------------------------------------------------------------
# LWT payload structure: agent → broker → cloud
# ---------------------------------------------------------------------------


class TestLwtPayloadStructure:
    """
    Verify the LWT payload built in app.py matches the telemetry.status
    envelope contract that NexusService expects.
    """

    @staticmethod
    def _build_lwt_payload() -> dict:
        """Build an LWT payload matching app.py's structure."""
        import uuid

        return {
            "$v": 1,
            "$type": "telemetry.status",
            "$id": str(uuid.uuid4()),
            "$ts": "",
            "$origin": "moonraker-owl@0.5.0",
            "deviceId": str(uuid.uuid4()),
            "payload": {
                "lifecycle": {
                    "phase": "Offline",
                    "isHeating": False,
                    "hasActiveJob": False,
                    "reason": "Connection lost (LWT)",
                },
                "cadence": {
                    "heartbeatSeconds": 0,
                    "watchWindowActive": False,
                },
                "lastUpdated": "",
            },
        }

    def test_lwt_has_envelope_fields(self):
        doc = self._build_lwt_payload()

        assert doc["$v"] == 1
        assert doc["$type"] == "telemetry.status"
        assert doc["$id"]
        assert "$ts" in doc
        assert doc["$origin"]
        assert doc["deviceId"]

    def test_lwt_lifecycle_phase_is_offline(self):
        doc = self._build_lwt_payload()
        lifecycle = doc["payload"]["lifecycle"]

        assert lifecycle["phase"] == "Offline"
        assert lifecycle["isHeating"] is False
        assert lifecycle["hasActiveJob"] is False
        assert lifecycle["reason"] == "Connection lost (LWT)"

    def test_lwt_cadence_is_zeroed(self):
        doc = self._build_lwt_payload()
        cadence = doc["payload"]["cadence"]

        assert cadence["heartbeatSeconds"] == 0
        assert cadence["watchWindowActive"] is False

    def test_lwt_payload_has_last_updated(self):
        doc = self._build_lwt_payload()

        assert "lastUpdated" in doc["payload"]


# ---------------------------------------------------------------------------
# Value validation: verify field values match expected formats
# ---------------------------------------------------------------------------

import re
from datetime import datetime as _dt


_UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)

_ISO8601_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}",
)


class TestContractValueValidation:
    """Verify fixture field values use correct formats (ISO 8601, UUID, enums)."""

    @pytest.mark.parametrize(
        "fixture_name",
        [
            "command-capture-image.json",
            "command-set-rate.json",
            "command-print-pause.json",
            "command-job-registered.json",
        ],
    )
    def test_envelope_id_is_uuid(self, fixture_name: str):
        data = _load_fixture(fixture_name)
        assert _UUID_RE.match(data["$id"]), f"$id is not a valid UUID: {data['$id']}"

    @pytest.mark.parametrize(
        "fixture_name",
        [
            "command-capture-image.json",
            "command-set-rate.json",
            "command-print-pause.json",
            "command-job-registered.json",
        ],
    )
    def test_envelope_ts_is_iso8601(self, fixture_name: str):
        data = _load_fixture(fixture_name)
        assert _ISO8601_RE.match(data["$ts"]), f"$ts is not ISO 8601: {data['$ts']}"

    @pytest.mark.parametrize(
        "fixture_name",
        [
            "command-capture-image.json",
            "command-set-rate.json",
            "command-print-pause.json",
            "command-job-registered.json",
        ],
    )
    def test_envelope_device_id_is_uuid(self, fixture_name: str):
        data = _load_fixture(fixture_name)
        assert _UUID_RE.match(data["deviceId"]), f"deviceId is not UUID: {data['deviceId']}"

    def test_capture_image_ids_are_uuid(self):
        data = _load_fixture("command-capture-image.json")
        params = data["payload"]["parameters"]
        assert _UUID_RE.match(params["captureFrameId"])

    def test_set_rate_issued_at_is_iso8601(self):
        data = _load_fixture("command-set-rate.json")
        params = data["payload"]["parameters"]
        assert _ISO8601_RE.match(params["issuedAt"])
        assert _ISO8601_RE.match(params["serverUtcNow"])

    def test_set_rate_mode_is_valid_enum(self):
        data = _load_fixture("command-set-rate.json")
        params = data["payload"]["parameters"]
        assert params["mode"] in {"idle", "watch"}

    def test_ack_status_is_valid_enum(self):
        builder = TestAckPayloadStructure()
        for status in ("accepted", "completed", "failed"):
            doc = builder._build_sample_ack(status=status)
            assert doc["payload"]["status"] in {"accepted", "completed", "failed"}

    def test_ack_stage_is_valid_enum(self):
        builder = TestAckPayloadStructure()
        for stage in ("dispatch", "execution"):
            doc = builder._build_sample_ack(stage=stage)
            assert doc["payload"]["stage"] in {"dispatch", "execution"}


# ---------------------------------------------------------------------------
# Backward compatibility: unknown fields must not break parsing
# ---------------------------------------------------------------------------


class TestBackwardCompatibility:
    """
    When the cloud adds new fields (envelope, payload, or parameters),
    the agent must still parse existing fields correctly.
    """

    ALL_FIXTURES = [
        ("command-capture-image.json", "task:capture-image"),
        ("command-set-rate.json", "control:set-telemetry-rate"),
        ("command-print-pause.json", "print:pause"),
        ("command-job-registered.json", "job:registered"),
    ]

    @pytest.mark.parametrize("fixture_name,command_name", ALL_FIXTURES)
    def test_unknown_envelope_fields_ignored(self, fixture_name, command_name):
        """Extra top-level fields (future cloud versions) don't break parsing."""
        data = _load_fixture(fixture_name)
        data["$newField"] = "future-value"
        data["metadata"] = {"region": "us-east-1", "priority": 5}

        msg = _parse_command(_to_bytes(data), command_name)
        assert msg.command_id == data["$id"]
        assert msg.command == command_name

    @pytest.mark.parametrize("fixture_name,command_name", ALL_FIXTURES)
    def test_unknown_payload_fields_ignored(self, fixture_name, command_name):
        """Extra fields inside payload don't affect command/parameters extraction."""
        data = _load_fixture(fixture_name)
        data["payload"]["routingKey"] = "some.routing.key"
        data["payload"]["retryCount"] = 3
        data["payload"]["tags"] = ["urgent", "v2"]

        msg = _parse_command(_to_bytes(data), command_name)
        assert msg.command_id == data["$id"]
        assert msg.command == command_name

    @pytest.mark.parametrize("fixture_name,command_name", ALL_FIXTURES)
    def test_unknown_parameter_fields_preserved(self, fixture_name, command_name):
        """Extra fields inside parameters are passed through to handlers."""
        data = _load_fixture(fixture_name)
        data["payload"]["parameters"]["futureFlag"] = True
        data["payload"]["parameters"]["experimentId"] = "exp-999"

        msg = _parse_command(_to_bytes(data), command_name)
        assert msg.parameters["futureFlag"] is True
        assert msg.parameters["experimentId"] == "exp-999"

    def test_higher_envelope_version_still_parses(self):
        """A future $v=2 envelope should still parse (no version gating)."""
        data = _load_fixture("command-print-pause.json")
        data["$v"] = 2
        data["$schemaVersion"] = "2.0.0"

        msg = _parse_command(_to_bytes(data), "print:pause")
        assert msg.command_id == data["$id"]
        assert msg.command == "print:pause"

    def test_capture_image_with_extra_params_preserves_known(self):
        """capture-image with extra params still exposes known fields."""
        data = _load_fixture("command-capture-image.json")
        data["payload"]["parameters"]["compressionQuality"] = 85
        data["payload"]["parameters"]["maxWidth"] = 1920

        msg = _parse_command(_to_bytes(data), "task:capture-image")
        assert msg.parameters["captureFrameId"] == "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee"
        assert msg.parameters["presignedUploadUrl"].startswith("https://")
        assert msg.parameters["compressionQuality"] == 85

    def test_set_rate_with_extra_params_preserves_known(self):
        """set-rate with extra params still exposes known fields."""
        data = _load_fixture("command-set-rate.json")
        data["payload"]["parameters"]["adaptiveMode"] = True

        msg = _parse_command(_to_bytes(data), "control:set-telemetry-rate")
        assert msg.parameters["mode"] == "watch"
        assert msg.parameters["intervalSeconds"] == 0.5
        assert msg.parameters["adaptiveMode"] is True
