"""Vendor-managed bootstrap defaults for the official Owl Cloud endpoints.

These values ship **inside the plugin's git tree** and are overwritten on every
plugin update (Moonraker runs ``git reset --hard`` for ``git_repo`` extensions).
Changing an endpoint is therefore an ordinary code change shipped with a new
plugin version — there is no ``install.sh`` hook and no local file rewrite. The
distribution channel (GitHub/Gitee) is independent of Owl Cloud, so a device can
pick up a new target even if the old domain is already gone.

Precedence: a value explicitly set under ``[cloud]`` in the user's TOML
(``~/printer_data/config/moonraker-owl.toml``) overrides the matching managed
default. That is the escape hatch for developers pointing the agent at a
custom/self-hosted backend. Ordinary installs leave these keys unset and always
track the official target as it moves.

See ``docs/audits/moonraker-config-update-mechanism-audit-2026-07-30.md``.
"""

from __future__ import annotations

# Official Owl Cloud HTTPS control-plane endpoint.
MANAGED_BASE_URL = "https://staging.mewcon.net"

# Official Owl Cloud MQTT broker (TLS on 8883).
MANAGED_BROKER_HOST = "mqtt.staging.mewcon.net"
MANAGED_BROKER_PORT = 8883
MANAGED_BROKER_USE_TLS = True
