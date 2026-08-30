"""The library hands back a default-initialised state when an appliance does
not answer, and those defaults are indistinguishable from readings."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from midea_nats_bridge.config import Credentials, DeviceConfig, Settings
from midea_nats_bridge.device import CommandNotDeliveredError, MideaBridge
from midea_nats_bridge.metrics import Metrics


class FakeState:
    def __init__(self, running: bool = False, mode: int = 1, online: bool = True) -> None:
        self.running = running
        self.mode = mode
        self.online = online


class FakeAppliance:
    """Stand-in for the library's LanDevice, whose own `online` says only that
    the network exchange worked."""

    def __init__(self, online: bool, state_online: bool = True) -> None:
        self.online = online
        self.state = FakeState(online=state_online)


def test_has_data_needs_both_online_flags() -> None:
    # `refresh()` does not raise when the appliance answers nothing; it leaves
    # state at mode 0 / fan_speed 40 / temperature 0. Those would reach a KNX
    # group address as an invalid mode and 0 °C. LanDevice.online alone does
    # not rule it out — the library raises that flag the moment an appliance
    # answers discovery, with the state still at its constructor defaults.
    assert MideaBridge._has_data(FakeAppliance(online=True)) is True
    assert MideaBridge._has_data(FakeAppliance(online=False)) is False
    assert MideaBridge._has_data(FakeAppliance(online=True, state_online=False)) is False


def test_has_data_is_false_when_the_flag_is_missing() -> None:
    # A library that stopped exposing `online` must not silently be treated as
    # always-fresh; absence means "cannot confirm", not "fine".
    class NoFlag:
        state: Any = object()

    assert MideaBridge._has_data(NoFlag()) is False


class ObedientAppliance:
    """Models apply()/refresh() honestly: refresh() overwrites the local state
    from what the appliance actually committed, so a refused command reverts."""

    def __init__(self, obeys: bool = True) -> None:
        self.online = True
        self.obeys = obeys
        self.refreshes = 0
        self.state = FakeState()
        self._committed = FakeState()

    def apply(self) -> None:
        if self.obeys:
            self._committed = FakeState(self.state.running, self.state.mode)

    def refresh(self) -> None:
        self.refreshes += 1
        self.state = FakeState(self._committed.running, self._committed.mode)


class SlowAppliance(ObedientAppliance):
    """Carries a command into its own state only after `after` re-reads."""

    def __init__(self, after: int) -> None:
        super().__init__()
        self.after = after
        self._pending: FakeState | None = None

    def apply(self) -> None:
        self._pending = FakeState(self.state.running, self.state.mode)

    def refresh(self) -> None:
        if self._pending is not None and self.refreshes + 1 >= self.after:
            self._committed = self._pending
            self._pending = None
        super().refresh()


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, dict[str, Any]]] = []

    def enqueue(self, _device: str, kind: str, _subject: str, payload: dict[str, Any]) -> None:
        self.published.append((kind, payload))


def _bridge(
    monkeypatch: pytest.MonkeyPatch, appliance: Any, delays: str = "0.01"
) -> tuple[MideaBridge, Metrics, FakePublisher]:
    monkeypatch.setattr(
        Settings, "read_device_credentials", lambda _self, _name: Credentials(token="t", key="k")
    )
    metrics, publisher = Metrics(), FakePublisher()
    bridge = MideaBridge(
        Settings(command_confirm_delays=delays),
        DeviceConfig(name="kg5", host="h"),
        publisher,  # type: ignore[arg-type]
        metrics,
    )
    bridge._appliance = appliance
    return bridge, metrics, publisher


async def _settle(bridge: MideaBridge) -> None:
    await asyncio.gather(*list(bridge._confirm_tasks))


def _confirmations(metrics: Metrics, function: str, outcome: str) -> float:
    counter = metrics.command_confirmations.labels(device="kg5", function=function, outcome=outcome)
    return float(counter._value.get())


def _pending(metrics: Metrics) -> float:
    return float(metrics.pending_commands.labels(device="kg5")._value.get())


async def test_confirmation_flags_a_command_the_appliance_refused(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The compressor lockout case: apply() succeeds, the appliance ignores it.
    appliance = ObedientAppliance(obeys=False)
    bridge, metrics, publisher = _bridge(monkeypatch, appliance, delays="0.01,0.01,0.01")

    await bridge.apply_command("power", True)
    await _settle(bridge)

    # Only the last attempt may declare a mismatch — every delay is spent first.
    assert appliance.refreshes == 3
    assert _confirmations(metrics, "power", "mismatch") == 1
    assert _confirmations(metrics, "power", "confirmed") == 0
    # The status subject must carry what the appliance does, not what we asked,
    # and only once — an intermediate publish would flap the group address.
    assert publisher.published.count(("state", {"power": False, "mode": 1, "locked": False})) == 1


async def test_confirmation_stops_at_the_first_agreement(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The point of backing off: a unit that needs a moment is confirmed as soon
    # as it catches up, without spending the rest of the schedule.
    appliance = SlowAppliance(after=3)
    bridge, metrics, _ = _bridge(monkeypatch, appliance, delays="0.01,0.01,0.01,0.01,0.01")

    await bridge.apply_command("power", True)
    await _settle(bridge)

    assert appliance.refreshes == 3
    assert _confirmations(metrics, "power", "confirmed") == 1
    assert _confirmations(metrics, "power", "mismatch") == 0


async def test_confirmation_accepts_a_command_that_took(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge, metrics, publisher = _bridge(monkeypatch, ObedientAppliance())

    await bridge.apply_command("power", True)
    await _settle(bridge)

    assert _confirmations(metrics, "power", "confirmed") == 1
    assert ("state", {"power": True, "mode": 1, "locked": False}) in publisher.published


async def test_a_newer_command_owns_the_outcome(monkeypatch: pytest.MonkeyPatch) -> None:
    # Basalte re-asserting within the confirmation window must not be reported
    # as a mismatch against the value it superseded.
    bridge, metrics, _ = _bridge(monkeypatch, ObedientAppliance(), delays="0.05")

    await bridge.apply_command("power", True)
    await bridge.apply_command("power", False)
    await _settle(bridge)

    assert _confirmations(metrics, "power", "superseded") == 1
    assert _confirmations(metrics, "power", "confirmed") == 1
    assert _confirmations(metrics, "power", "mismatch") == 0


async def test_a_command_the_appliance_never_got_is_held(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # The failure this exists for: the appliance drops off the WLAN, the room
    # alarm switches on, and the only telegram anyone will send is lost.
    bridge, metrics, _ = _bridge(monkeypatch, ObedientAppliance())
    bridge._appliance = None

    with pytest.raises(CommandNotDeliveredError):
        await bridge.apply_command("power", True)

    assert bridge._pending == {"power": True}
    assert _pending(metrics) == 1


async def test_a_failed_round_trip_holds_the_command(monkeypatch: pytest.MonkeyPatch) -> None:
    # Connected is not delivered: the socket can still die mid-apply.
    class BrokenAppliance(ObedientAppliance):
        def apply(self) -> None:
            raise OSError("timed out")

    bridge, _, _ = _bridge(monkeypatch, BrokenAppliance())

    with pytest.raises(CommandNotDeliveredError):
        await bridge.apply_command("power", True)

    assert bridge._pending == {"power": True}


async def test_a_held_command_is_re_asserted_on_reconnect(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    appliance = ObedientAppliance()
    bridge, metrics, publisher = _bridge(monkeypatch, appliance)
    bridge._appliance = None
    with pytest.raises(CommandNotDeliveredError):
        await bridge.apply_command("power", True)

    bridge._appliance = appliance  # as the supervisor does after a reconnect
    await bridge._reassert_pending()
    await _settle(bridge)

    assert appliance._committed.running is True
    assert bridge._pending == {}
    assert _pending(metrics) == 0
    assert _confirmations(metrics, "power", "confirmed") == 1
    assert ("state", {"power": True, "mode": 1, "locked": False}) in publisher.published


async def test_a_newer_command_replaces_the_held_one(monkeypatch: pytest.MonkeyPatch) -> None:
    # Switched back off while still unreachable: the appliance must not be woken
    # by the superseded value once it answers again.
    bridge, _, _ = _bridge(monkeypatch, ObedientAppliance())
    bridge._appliance = None

    for value in (True, False):
        with pytest.raises(CommandNotDeliveredError):
            await bridge.apply_command("power", value)

    assert bridge._pending == {"power": False}


async def test_a_locked_appliance_drops_held_commands(monkeypatch: pytest.MonkeyPatch) -> None:
    # The lock holds the appliance at its current setting, so a queued command
    # is swallowed like any other rather than fired late.
    appliance = ObedientAppliance()
    bridge, _, _ = _bridge(monkeypatch, appliance)
    bridge._appliance = None
    with pytest.raises(CommandNotDeliveredError):
        await bridge.apply_command("power", True)

    bridge.set_lock(True)
    bridge._appliance = appliance
    await bridge._reassert_pending()

    assert bridge._pending == {}
    assert appliance._committed.running is False


async def test_refresh_cannot_interleave_with_apply(monkeypatch: pytest.MonkeyPatch) -> None:
    """refresh() overwrites `state` wholesale, so an overlapping refresh would
    discard the attribute set for the pending apply()."""

    class OrderedAppliance(ObedientAppliance):
        def __init__(self) -> None:
            super().__init__()
            self.events: list[str] = []

        def apply(self) -> None:
            self.events.append("apply-start")
            time.sleep(0.05)
            super().apply()
            self.events.append("apply-end")

        def refresh(self) -> None:
            self.events.append("refresh")
            super().refresh()

    appliance = OrderedAppliance()
    bridge, _, _ = _bridge(monkeypatch, appliance, delays="")

    await asyncio.gather(bridge.apply_command("power", True), bridge._refresh())

    between = appliance.events[appliance.events.index("apply-start") :]
    assert "refresh" not in between[: between.index("apply-end")]
