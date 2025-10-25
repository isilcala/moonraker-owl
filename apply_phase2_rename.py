"""Script to apply Phase 2 channel rename from metrics to sensors."""

# Read the file
with open('moonraker_owl/telemetry.py', 'r', encoding='utf-8') as f:
    content = f.read()

# Apply replacements for Phase 2 channel rename - fixed to not break syntax
replacements = [
    ('"metrics": f"{self._base_topic}/metrics"', '"sensors": f"{self._base_topic}/sensors"  # Phase 2: Renamed from "metrics"'),
    ('"metrics": 0', '"sensors": 0  # Phase 2: Renamed from "metrics"'),
    ('TelemetryStateCache(("overview", "metrics")', 'TelemetryStateCache(("overview", "sensors")  # Phase 2'),
    ('return self._channel_topics["metrics"]', 'return self._channel_topics["sensors"]  # Phase 2: Renamed from "metrics"'),
    ('"metrics", reason="heater refresh"', '"sensors", reason="heater refresh"  # Phase 2: Renamed from "metrics"'),
    ('self._state_cache.peek_payload("metrics")', 'self._state_cache.peek_payload("sensors")  # Phase 2'),
    ('"metrics", normalized.metrics, raw_json, force_full=force_full', '"sensors", normalized.metrics, raw_json, force_full=force_full  # Phase 2: Publish on "sensors"'),
    ('if channel == "metrics":', 'if channel == "sensors":  # Phase 2: Renamed from "metrics"'),
    ('in {"overview", "metrics"}', 'in {"overview", "sensors"}  # Phase 2: Added "sensors"'),
    ('channel == "metrics" and isinstance', 'channel == "sensors" and isinstance  # Phase 2: Renamed from "metrics"'),
    ('document["metrics"] = payload', 'document["sensors"] = payload  # Phase 2'),
]

for old, new in replacements:
    content = content.replace(old, new)

# Write back
with open('moonraker_owl/telemetry.py', 'w', encoding='utf-8') as f:
    f.write(content)

print('✅ Phase 2 channel rename complete!')
