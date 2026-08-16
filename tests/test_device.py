"""The library hands back a default-initialised state when an appliance does
not answer, and those defaults are indistinguishable from readings."""

from __future__ import annotations

from typing import Any

from midea_nats_bridge.device import MideaBridge


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
