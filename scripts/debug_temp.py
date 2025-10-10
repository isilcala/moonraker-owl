import json
from pathlib import Path

from moonraker_owl.telemetry_normalizer import TelemetryNormalizer

repo_root = Path(__file__).resolve().parents[2]
sample_path = repo_root / "docs" / "examples" / "moonraker-sample-printing.json"
sample = json.loads(sample_path.read_text(encoding="utf-8"))

normalizer = TelemetryNormalizer()
channels = normalizer.ingest(sample)
print("first", channels.telemetry)

updated = {
    "params": [
        {
            "status": {
                "extruder": {
                    "temperature": 210.0,
                    "target": 215.0,
                }
            }
        }
    ]
}
channels2 = normalizer.ingest(updated)
print("second", channels2.telemetry)
