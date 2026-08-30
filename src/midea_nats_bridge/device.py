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


class CommandNotDeliveredError(Exception):
    """The appliance never received the command; it is held as desired state."""


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
        # Serialises every appliance round-trip. refresh() overwrites `state`
        # wholesale, so an overlapping apply() would write back the refreshed
        # values instead of the commanded one.
        self._io_lock = asyncio.Lock()
        # Latest commanded value per function, so a confirmation that has been
        # overtaken by a newer command reports nothing.
        self._commanded: dict[str, Any] = {}
        # Commands the appliance never received, held as the desired state and
        # re-sent once it answers again. An upstream controller reacting to a
        # room sensor only ever announces changes, so a command dropped while
        # the appliance is off the WLAN would stay lost until the next edge.
        self._pending: dict[str, Any] = {}
        self._confirm_tasks: set[asyncio.Task[None]] = set()

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
        leaves `state` at its constructor defaults. Those defaults are
        indistinguishable from readings — mode 0, fan_speed 40,
        target_humidity 50, humidity 45, temperature 0 — so publishing them
        would write a plausible-looking lie to NATS and onward to KNX.

        Both flags are needed. `LanDevice.online` is a network-level flag: the
        library raises it the moment an appliance answers discovery, before any
        status response has been parsed, and leaves it up while a refresh
        quietly returns nothing. Only the appliance's own flag means "a status
        response was parsed into this state".
        """
        return bool(getattr(appliance, "online", False)) and bool(
            getattr(getattr(appliance, "state", None), "online", False)
        )

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
        for task in (self._poll_task, *self._confirm_tasks):
            if task is None:
                continue
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        self._poll_task = None
        self._confirm_tasks.clear()
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
                await self._reassert_pending()

            await asyncio.sleep(self._settings.poll_interval)
            await self._poll()

    async def _refresh(self) -> str | None:
        """Refresh under the I/O lock; returns a failure reason, or None on success."""
        if self._appliance is None:
            return "appliance not connected"
        try:
            async with self._io_lock:
                await asyncio.to_thread(self._appliance.refresh)
        except Exception as exc:
            return str(exc)
        # refresh() stays silent when the appliance answers nothing, so a
        # successful call is not proof of fresh data.
        if not self._has_data(self._appliance):
            return "appliance stopped answering"
        return None

    async def _poll(self) -> None:
        if self._appliance is None:
            return
        reason = await self._refresh()
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

    async def apply_command(self, function: str, value: Any) -> None:
        """Translate one validated command into a library call.

        The library applies attributes in one `apply()` round-trip and publishes
        no optimistic state; the confirmation scheduled here re-reads the
        appliance and is what makes the result visible.

        Raises CommandNotDeliveredError when the appliance never received the command —
        it is unreachable, or the round-trip failed. The value is then held as
        the desired state and re-sent on the next successful connect, because
        the sender announces changes and would not repeat itself.
        """
        attr = _COMMAND_ATTRS.get(function)
        if attr is None:
            raise ValueError(f"unknown command function {function!r}")
        # Set before the round-trip: this is the latest intent either way, and a
        # confirmation still waiting on the previous value must see itself
        # superseded even when this command cannot be delivered.
        self._commanded[function] = value

        if self._appliance is None:
            self._defer(function, value)
            raise CommandNotDeliveredError("appliance not connected")
        try:
            async with self._io_lock:
                await asyncio.to_thread(self._apply_blocking, attr, value)
        except Exception as exc:
            self._defer(function, value)
            raise CommandNotDeliveredError(str(exc)) from exc

        self._pending.pop(function, None)
        self._track_pending()
        self._schedule_confirmation(function, value)

    def _apply_blocking(self, attr: str, value: Any) -> None:
        setattr(self._appliance.state, attr, value)
        self._appliance.apply()

    # --- undelivered commands ---------------------------------------------

    def _track_pending(self) -> None:
        self._metrics.pending_commands.labels(device=self._name).set(len(self._pending))

    def _defer(self, function: str, value: Any) -> None:
        self._pending[function] = value
        self._track_pending()

    async def _reassert_pending(self) -> None:
        """Re-send commands the appliance never received, in the order they came.

        Called once per successful connect. A command that fails again is left
        pending for the next one; nothing else re-tries, so a permanently
        unreachable appliance costs one round-trip per reconnect.
        """
        if not self._pending:
            return
        if self._locked:
            # The lock holds the appliance at its current setting, so a queued
            # command is swallowed like any other rather than fired late.
            logger.info(
                "[%s] dropping %d held command(s): appliance locked", self._name, len(self._pending)
            )
            self._pending.clear()
            self._track_pending()
            return

        for function, value in list(self._pending.items()):
            logger.info("[%s] re-asserting held command %s=%r", self._name, function, value)
            try:
                await self.apply_command(function, value)
            except CommandNotDeliveredError as exc:
                logger.warning(
                    "[%s] re-assert of %s failed, still held: %s", self._name, function, exc
                )
                return
            self._metrics.command_reasserts.labels(device=self._name, function=function).inc()

    # --- command confirmation ---------------------------------------------

    def _schedule_confirmation(self, function: str, value: Any) -> None:
        delays = self._settings.command_confirm_delays_list
        if not delays:
            return
        task = asyncio.create_task(self._confirm(function, value, delays))
        self._confirm_tasks.add(task)
        task.add_done_callback(self._confirm_tasks.discard)

    def _count_confirmation(self, function: str, outcome: str) -> None:
        self._metrics.command_confirmations.labels(
            device=self._name, function=function, outcome=outcome
        ).inc()

    def _publish_confirmation(self) -> None:
        self._metrics.last_message_ts.labels(device=self._name).set(time.time())
        self._publish()

    async def _confirm(self, function: str, value: Any, delays: list[float]) -> None:
        """Re-read after a command until the appliance agrees, then report.

        `apply()` is fire-and-forget: the appliance acknowledges nothing, so a
        refused command — a compressor lockout shortly after a power change, for
        instance — looks exactly like a successful one until a later poll happens
        to contradict it.

        How long a unit takes to carry a command into its own state is not
        specified anywhere, so this backs off across `delays` and settles on the
        first agreement instead of betting on a single interval. Only the last
        attempt may declare a mismatch; intermediate disagreements publish
        nothing, because writing the superseded value to a status address would
        make the group address flap on its way to the right answer.
        """
        reason: str | None = None
        for delay in delays:
            await asyncio.sleep(delay)
            if self._stopping:
                return
            if self._commanded.get(function) != value:
                # A newer command for this function landed while we waited; that
                # one owns the outcome.
                self._count_confirmation(function, "superseded")
                return

            reason = await self._refresh()
            if reason is not None:
                continue
            if getattr(self._appliance.state, _COMMAND_ATTRS[function], None) == value:
                self._count_confirmation(function, "confirmed")
                self._publish_confirmation()
                return

        if reason is not None:
            self._count_confirmation(function, "unavailable")
            logger.warning("[%s] confirmation poll for %s failed: %s", self._name, function, reason)
            return

        observed = getattr(self._appliance.state, _COMMAND_ATTRS[function], None)
        self._count_confirmation(function, "mismatch")
        logger.warning(
            "[%s] %s did not take within %.0fs: commanded %r, appliance reports %r",
            self._name,
            function,
            sum(delays),
            value,
            observed,
        )
        # The status subject must carry what the appliance actually does.
        self._publish_confirmation()
