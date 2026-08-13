"""Unit tests for the Midea-dialect normalization."""

from __future__ import annotations

from typing import Any

from midea_nats_bridge.normalize import normalize_environment, normalize_state


class FakeState:
    """Property-bag stand-in; raising attrs simulate fields a model lacks."""

    def __init__(self, **fields: Any) -> None:
        self._fields = fields

    def __getattr__(self, name: str) -> Any:
        if name in self._fields:
            value = self._fields[name]
            if isinstance(value, Exception):
                raise value
            return value
        raise AttributeError(name)


def test_state_collects_switches_and_integers() -> None:
    state = normalize_state(
        FakeState(
            running=True,
            mode=1,
            fan_speed=40,
            target_humidity=55,
            tank_full=False,
            ion_mode=False,
            sleep_mode=False,
            pump=False,
            filter_indicator=False,
            defrosting=False,
            error_code=0,
        )
    )
    assert state == {
        "power": True,
        "ion": False,
        "sleep": False,
        "pump": False,
        "tank_full": False,
        "filter_indicator": False,
        "defrosting": False,
        "mode": 1,
        "fan_speed": 40,
        "target_humidity": 55,
        "error_code": 0,
    }


def test_state_omits_unsupported_fields() -> None:
    # A model without a pump raises rather than returning None.
    state = normalize_state(
        FakeState(running=True, mode=2, pump=AttributeError("pump"), tank_full=True)
    )
    assert "pump" not in state
    assert state == {"power": True, "mode": 2, "tank_full": True}


def test_mode_and_fan_speed_pass_through_untranslated() -> None:
    # Deliberately no enum mapping: the device's own integers carry to KNX.
    state = normalize_state(FakeState(mode=15, fan_speed=127))
    assert state["mode"] == 15
    assert state["fan_speed"] == 127


def test_environment_rounds_temperature_and_drops_negatives() -> None:
    assert normalize_environment(
        FakeState(current_humidity=62, current_temperature=21.4499, tank_level=30)
    ) == {"humidity": 62, "temperature_c": 21.4, "tank_level": 30}

    # Sentinels for "not measured" arrive as negatives on some models.
    assert (
        normalize_environment(
            FakeState(current_humidity=-1, current_temperature=AttributeError("t"), tank_level=-1)
        )
        == {}
    )
