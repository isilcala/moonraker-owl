"""Protocol definitions for printer adapters and callbacks."""

from __future__ import annotations

from typing import Any, Awaitable, Callable, Mapping, Optional, Protocol


CallbackType = Callable[[dict[str, Any]], Awaitable[None] | None]


class PrinterAdapter(Protocol):
    """Minimal contract for components that surface printer connectivity."""

    async def start(self, callback: CallbackType) -> None:
        """Begin streaming printer events to the provided callback."""
        ...

    def remove_callback(self, callback: CallbackType) -> None:
        """Stop routing events to the provided callback."""
        ...

    def set_subscription_objects(
        self, objects: Mapping[str, Optional[list[str]]] | None
    ) -> None:
        """Declare which status objects should be streamed on connect."""
        ...

    async def fetch_printer_state(
        self,
        objects: Optional[Mapping[str, Optional[list[str]]]] = None,
        timeout: float = 5.0,
    ) -> dict[str, Any]:
        """Retrieve the current printer status snapshot.

        Args:
            objects: Moonraker objects to query (None = all)
            timeout: Request timeout in seconds
        """
        ...

    async def resubscribe(self) -> None:
        """Reapply any underlying status subscriptions."""
        ...

    async def execute_print_action(self, action: str) -> None:
        """Execute a high-level print control action (pause, resume, cancel)."""
        ...

    async def aclose(self) -> None:
        """Close any underlying resources."""
        ...
