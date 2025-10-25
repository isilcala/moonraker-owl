"""Fix specific syntax errors in telemetry.py."""

with open('moonraker_owl/telemetry.py', 'r', encoding='utf-8') as f:
    lines = f.readlines()

# Fix line 126 (index 125): Move comment after comma
lines[125] = '            "sensors": f"{self._base_topic}/sensors",  # Phase 2: Renamed from "metrics"\n'

# Fix line 132 (index 131): Move comment after comma  
lines[131] = '         "sensors": 0,  # Phase 2: Renamed from "metrics"\n'

# Fix line 152 (index 151): Fix TelemetryStateCache call
lines[151] = '        self._state_cache = TelemetryStateCache(("overview", "sensors"), self._hasher)  # Phase 2\n'

# Fix line 485 (index 484): Fix condition check
lines[484] = '        if channel in {"overview", "sensors"} and not isinstance(payload, dict):  # Phase 2: Added "sensors"\n'

# Fix line 493 (index 492): Fix condition check
lines[492] = '     if channel == "sensors" and isinstance(payload, dict):  # Phase 2: Renamed from "metrics"\n'

with open('moonraker_owl/telemetry.py', 'w', encoding='utf-8') as f:
    f.writelines(lines)

print('✅ Syntax errors fixed!')
