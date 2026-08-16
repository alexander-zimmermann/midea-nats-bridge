"""Appliance connection owner: LAN polling, reconnect loop, command apply.

midea-beautiful-air is synchronous (`requests` under the hood) and offers no
push channel, so every call leaves the event loop via asyncio.to_thread and the
poll loop is the only source of state — unlike the Dyson bridge, where polling
merely backstops the device's own STATE-CHANGE pushes.
"""

from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from typing import Any

from midea_beautiful import appliance_state

from .config import DeviceConfig, Settings
from .metrics import Metrics
from .normalize import normalize_environment, normalize_state
from .publisher import Publisher

logger = logging.getLogger(__name__)

_RECONNECT_BACKOFF_START_SECONDS = 5.0
_RECONNECT_BACKOFF_MAX_SECONDS = 300.0

# `lock` is not an appliance capability — it is a bridge-side flag that makes
# the other commands no-ops, so a KNX/Basalte lock can hold the appliance at
# its current setting. Mirrors the Dyson bridge.
COMMAND_FUNCTIONS = ("power", "mode", "fan_speed", "target_humidity", "lock")
LOCKABLE_FUNCTIONS = tuple(f for f in COMMAND_FUNCTIONS if f != "lock")

# Attribute on the library's appliance object for each command function.
_COMMAND_ATTRS = {
    "power": "running",
    "mode": "mode",
    "fan_speed": "fan_speed",
    "target_humidity": "target_humidity",
}


class MideaBridge:
    """Owns one appliance's connection and publishes its normalized state to NATS."""

    def __init__(
        self,
        settings: Settings,
        config: DeviceConfig,
        publisher: Publisher,
        metrics: Metrics,
    ) -> None:
        self._settings = settings
        self._config = config
        self._name = config.name
        self._publisher = publisher
        self._metrics = metrics
        self._credentials = settings.read_device_credentials(config.name)
        self._appliance: Any = None
        self._poll_task: asyncio.Task[None] | None = None
        self._stopping = False
        self._locked = False
        # Last published payload per kind, so an unchanged poll stays off NATS.
        self._last: dict[str, dict[str, Any]] = {}

    @property
    def name(self) -> str:
        return self._config.name

    @property
    def is_connected(self) -> bool:
        """Holding a handle is not the same as having data — see _has_data()."""
        return self._appliance is not None and self._has_data(self._appliance)

    @staticmethod
    def _has_data(appliance: Any) -> bool:
        """Whether the library actually read the appliance, rather than
        handing back a default-initialised state.

        `refresh()` does not raise when the appliance answers nothing; it
        leaves `state` at its constructor defaults and `online` False. Those
        defaults are indistinguishable from readings — mode 0, fan_speed 40,
        target_humidity 50, humidity 45, temperature 0 — so publishing them
        would write a plausible-looking lie to NATS and onward to KNX.
        """
        return bool(getattr(appliance, "online", False))

    @property
    def locked(self) -> bool:
        return self._locked

    async def restore_lock(self) -> None:
        """Re-read the lock flag from the last archived state message.

        The flag only ever arrives over NATS, so without this a pod restart
        would silently unlock the appliance while the status GA still read true.
        """
        last = await self._publisher.last_message(self._config.state_subject)
        if last is not None and isinstance(last.get("locked"), bool):
            self._locked = last["locked"]
            if self._locked:
                logger.info("[%s] restored lock from last state message", self._name)

    def set_lock(self, locked: bool) -> None:
        """Set the lock and publish state at once, so the status GA follows."""
        self._locked = locked
        if self._appliance is not None:
            self._publish(force=True)

    async def start(self) -> None:
        self._poll_task = asyncio.create_task(self._supervise())

    async def stop(self) -> None:
        self._stopping = True
        if self._poll_task is not None:
            self._poll_task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._poll_task
            self._poll_task = None
        self._metrics.midea_connected.labels(device=self._name).set(0)

    # --- connection & polling -------------------------------------------

    def _connect(self) -> Any:
        """Blocking; call via to_thread. Returns a refreshed appliance handle."""
        return appliance_state(
            address=self._config.host,
            token=self._credentials.token,
            key=self._credentials.key,
        )

    async def _supervise(self) -> None:
        """Single loop owning connect, reconnect-with-backoff, and periodic polls."""
        backoff = _RECONNECT_BACKOFF_START_SECONDS
        while not self._stopping:
            if self._appliance is None:
                reason: str | None = None
                try:
                    appliance = await asyncio.to_thread(self._connect)
                    # A handle without data is not a connection: the library
                    # answers with a default-initialised state rather than
                    # raising, and those defaults read like measurements.
                    if not self._has_data(appliance):
                        reason = "appliance did not answer the initial refresh"
                    else:
                        self._appliance = appliance
                except Exception as exc:
                    reason = str(exc)
                if reason is not None:
                    self._metrics.midea_connected.labels(device=self._name).set(0)
                    self._metrics.reconnects.labels(device=self._name, outcome="error").inc()
                    logger.warning(
                        "[%s] connect to %s failed, retrying in %.0fs: %s",
                        self._name,
                        self._config.host,
                        backoff,
                        reason,
                    )
                    await asyncio.sleep(backoff)
                    backoff = min(backoff * 2, _RECONNECT_BACKOFF_MAX_SECONDS)
                    continue
                self._metrics.reconnects.labels(device=self._name, outcome="ok").inc()
                self._metrics.midea_connected.labels(device=self._name).set(1)
                backoff = _RECONNECT_BACKOFF_START_SECONDS
                logger.info("[%s] connected: %s", self._name, self._config.host)
                self._publish()

            await asyncio.sleep(self._settings.poll_interval)
            await self._poll()

    async def _poll(self) -> None:
        if self._appliance is None:
            return
        reason: str | None = None
        try:
            await asyncio.to_thread(self._appliance.refresh)
            # refresh() stays silent when the appliance answers nothing, so a
            # successful call is not proof of fresh data.
            if not self._has_data(self._appliance):
                reason = "appliance stopped answering"
        except Exception as exc:
            reason = str(exc)
        if reason is not None:
            self._metrics.poll_errors.labels(device=self._name).inc()
            logger.warning("[%s] poll failed: %s", self._name, reason)
            # Drop the handle so the supervisor reconnects rather than polling
            # a dead socket forever.
            self._appliance = None
            self._metrics.midea_connected.labels(device=self._name).set(0)
            return
        self._metrics.last_message_ts.labels(device=self._name).set(time.time())
        self._publish()

    # --- appliance -> NATS ------------------------------------------------

    def _publish(self, force: bool = False) -> None:
        """Publish state and environment, skipping payloads that did not change.

        The appliance is polled on a fixed interval but most of what it reports
        is static between runs; republishing it would only add noise to NATS and
        to the KNX writer's change detection.

        Never publishes without confirmed data. The callers already check, but
        this is the last point before a value leaves the process, and the cost
        of a stale default reaching a group address is a plausible-looking lie.
        """
        if self._appliance is None or not self._has_data(self._appliance):
            return

        state = normalize_state(self._appliance.state)
        state["locked"] = self._locked
        environment = normalize_environment(self._appliance.state)

        self._metrics.tank_full.labels(device=self._name).set(1 if state.get("tank_full") else 0)
        if "humidity" in environment:
            self._metrics.humidity.labels(device=self._name).set(environment["humidity"])

        for kind, subject, payload in (
            ("state", self._config.state_subject, state),
            ("environment", self._config.environment_subject, environment),
        ):
            if not payload:
                continue
            self._metrics.messages_received.labels(device=self._name, kind=kind).inc()
            if not force and self._last.get(kind) == payload:
                continue
            self._last[kind] = dict(payload)
            self._publisher.enqueue(self._name, kind, subject, payload)

    # --- NATS -> appliance ------------------------------------------------

    def apply_command(self, function: str, value: Any) -> None:
        """Translate one validated command into a library call (blocking).

        The library applies attributes in one `apply()` round-trip; the next
        poll reports the result, so no optimistic state is published here.
        """
        if self._appliance is None:
            raise RuntimeError("appliance not connected")
        attr = _COMMAND_ATTRS.get(function)
        if attr is None:
            raise ValueError(f"unknown command function {function!r}")
        setattr(self._appliance.state, attr, value)
        self._appliance.apply()
