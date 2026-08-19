"""Collection-specific policies for MongoDB change-stream events."""

from dataclasses import dataclass
from typing import Awaitable, Callable, Protocol


@dataclass(frozen=True)
class NotificationDependencies:
    """Transport callbacks shared by collection handlers."""

    push_to_slack: Callable[[str, str], Awaitable[bool]]
    notify_get: Callable[[], Awaitable[None]]
    run_async: Callable[[Awaitable], object]
    now_ms: Callable[[], float]


class CollectionHandler(Protocol):
    """Common contract implemented by each collection handler."""

    def __call__(self, database, operation: str, document: dict,
                 notifications: NotificationDependencies):
        """Process a change, doing nothing for unsupported operations."""
