"""Metadata command handlers (system-refresh)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from ..types import CommandMessage, CommandProcessingError

LOGGER = logging.getLogger(__name__)


class MetadataCommandsMixin:
    """Mixin providing metadata-related command handlers."""

    def _execute_metadata_system_refresh(
        self, message: CommandMessage
    ) -> Dict[str, Any]:
        """Handle metadata:system-refresh command.

        Triggers an immediate metadata report, bypassing the 24h periodic
        wait. Used for on-demand sensor discovery after mid-session hardware
        changes (e.g. user adds a new thermistor while the agent is running).
        """
        if self._metadata_reporter is None:
            raise CommandProcessingError(
                "Metadata reporter unavailable",
                code="metadata_unavailable",
                command_id=message.command_id,
            )

        self._metadata_reporter.force_report_now()

        return {"success": True, "action": "metadata_refresh_triggered"}
