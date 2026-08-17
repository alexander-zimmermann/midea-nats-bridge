"""Command subscription: midea.<device>.command.<function> {"value": ...} -> appliance."""

from __future__ import annotations

import json
import logging
from typing import Any

from nats.aio.msg import Msg

from .config import Settings
from .device import COMMAND_FUNCTIONS, MideaBridge
from .metrics import Metrics
from .publisher import Publisher

logger = logging.getLogger(__name__)

# Library bounds, not device capabilities. A given dehumidifier answers to a
# narrower set (see normalize.py); these only keep obvious nonsense off the
# wire. Values are rejected rather than clamped, because a clamped command
# looks like it worked while doing something else.
_MODE_MAX = 15
_FAN_SPEED_MAX = 127
_HUMIDITY_MIN = 0
_HUMIDITY_MAX = 100

_INT_RANGES = {
    "mode": (0, _MODE_MAX),
    "fan_speed": (0, _FAN_SPEED_MAX),
    "target_humidity": (_HUMIDITY_MIN, _HUMIDITY_MAX),
}


def split_subject(subject: str) -> tuple[str, str]:
    """Split midea.<device>.command.<function> into (device, function).

    Raises ValueError on anything that doesn't match, so a stray subject can
    never be routed to the wrong appliance.
    """
    parts = subject.split(".")
    if len(parts) != 4 or parts[2] != "command":
        raise ValueError(f"malformed command subject {subject!r}")
    return parts[1], parts[3]


def parse_command(subject: str, data: bytes) -> tuple[str, str, Any]:
    """Validate subject + payload; returns (device, function, value) or raises ValueError."""
    device, function = split_subject(subject)
    if function not in COMMAND_FUNCTIONS:
        raise ValueError(f"unknown command function {function!r}")

    try:
        payload = json.loads(data)
    except json.JSONDecodeError as exc:
        raise ValueError(f"invalid JSON payload: {exc}") from exc
    if not isinstance(payload, dict) or "value" not in payload:
        raise ValueError('payload must be an object with a "value" field')
    value = payload["value"]

    if function in _INT_RANGES:
        low, high = _INT_RANGES[function]
        # Bools rejected: True would silently become 1.
        if isinstance(value, bool) or not isinstance(value, int | float):
            raise ValueError(f"{function} must be a number, got {value!r}")
        value = int(value)
        if not low <= value <= high:
            raise ValueError(f"{function} must be {low}..{high}, got {value}")
        return device, function, value

    # power and lock are switches; accept bool or 0/1 (DPT 1.001 decodes to
    # bool, but tolerate numeric writes from manual `nats pub` testing).
    if isinstance(value, bool):
        return device, function, value
    if isinstance(value, int | float) and value in (0, 1):
        return device, function, bool(value)
    raise ValueError(f"{function} must be a boolean, got {value!r}")


class CommandHandler:
    """One wildcard subscription for every device; routes by the subject's device token."""

    def __init__(
        self,
        settings: Settings,
        bridges: dict[str, MideaBridge],
        publisher: Publisher,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._bridges = bridges
        self._publisher = publisher
        self._metrics = metrics

    async def start(self) -> None:
        await self._publisher.subscribe_core(
            self._settings.command_subject_filter, self._on_command
        )

    def _count(self, device: str, function: str, outcome: str) -> None:
        self._metrics.commands.labels(device=device, function=function, outcome=outcome).inc()

    async def _on_command(self, msg: Msg) -> None:
        # Best-effort labels for the failure paths; parse_command() refines them.
        try:
            device, function = split_subject(msg.subject)
        except ValueError:
            device, function = "unknown", "unknown"

        try:
            device, function, value = parse_command(msg.subject, msg.data)
        except ValueError as exc:
            self._count(device, function, "invalid")
            logger.warning("invalid command on %s: %s", msg.subject, exc)
            return

        bridge = self._bridges.get(device)
        if bridge is None:
            self._count(device, function, "unknown_device")
            logger.warning("command for unknown device %r on %s", device, msg.subject)
            return

        if function == "lock":
            # Bridge-side flag, no appliance I/O — stays on the event loop so
            # the immediate state publish is safe.
            bridge.set_lock(bool(value))
            self._count(device, function, "ok")
            logger.info("[%s] lock %s", device, "set" if value else "cleared")
            return

        if bridge.locked:
            # Counted, but deliberately no status write: the status GA reflects
            # the lock itself, not each command it swallows.
            self._count(device, function, "locked")
            logger.info("[%s] command %s=%r ignored: appliance locked", device, function, value)
            return

        try:
            await bridge.apply_command(function, value)
        except Exception as exc:
            self._count(device, function, "error")
            logger.warning("[%s] command %s=%r failed: %s", device, function, value, exc)
            return

        self._count(device, function, "ok")
        logger.info("[%s] command applied: %s=%r", device, function, value)
