"""The library hands back a default-initialised state when an appliance does
not answer, and those defaults are indistinguishable from readings."""

from __future__ import annotations

import asyncio
import time
from typing import Any

import pytest

from midea_nats_bridge.config import Credentials, DeviceConfig, Settings
from midea_nats_bridge.device import MideaBridge
from midea_nats_bridge.metrics import Metrics


class FakeAppliance:
    """Stand-in for the library's LanDevice."""

    def __init__(self, online: bool) -> None:
        self.online = online
        self.state = object()


def test_has_data_follows_the_online_flag() -> None:
    # `refresh()` does not raise when the appliance answers nothing; it leaves
    # state at mode 0 / fan_speed 40 / temperature 0 and online False. Those
    # would reach a KNX group address as an invalid mode and 0 °C.
    assert MideaBridge._has_data(FakeAppliance(online=True)) is True
    assert MideaBridge._has_data(FakeAppliance(online=False)) is False


def test_has_data_is_false_when_the_flag_is_missing() -> None:
    # A library that stopped exposing `online` must not silently be treated as
    # always-fresh; absence means "cannot confirm", not "fine".
    class NoFlag:
        state: Any = object()

    assert MideaBridge._has_data(NoFlag()) is False


class FakeState:
    def __init__(self, running: bool = False, mode: int = 1) -> None:
        self.running = running
        self.mode = mode


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
