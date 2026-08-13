"""Normalize midea-beautiful-air appliance state into flat scalar JSON.

The knx-nats-bridge writer can only extract named scalar fields (no arrays, no
transforms), so anything that needs resolving is resolved here.

On enums: `mode` and `fan_speed` are passed through as the device's own
integers, deliberately untranslated. The library accepts mode 0-15 and
fan_speed 0-127, but those are library bounds, not device capabilities — a
given dehumidifier answers to a handful of values. Inventing a tidy 1..4 enum
here would mean guessing which raw value each rung maps to, and a wrong guess
is invisible: the device simply does something else than the KNX label says.
The KNX group addresses use DPT 5.010 (0..255), so raw values carry fine.
Confirm the accepted set against a real appliance (`midea-cloud-fetch` prints
`capabilities`) and document it in the README before relying on specific rungs.
"""

from __future__ import annotations

import logging
from typing import Any, Protocol

logger = logging.getLogger(__name__)


class DehumidifierState(Protocol):
    """The subset of midea-beautiful-air's DehumidifierAppliance surface we read."""

    @property
    def running(self) -> bool: ...
    @property
    def mode(self) -> int: ...
    @property
    def fan_speed(self) -> int: ...
    @property
    def target_humidity(self) -> int: ...
    @property
    def ion_mode(self) -> bool: ...
    @property
    def sleep_mode(self) -> bool: ...
    @property
    def pump(self) -> bool: ...
    @property
    def tank_full(self) -> bool: ...
    @property
    def tank_level(self) -> int: ...
    @property
    def current_humidity(self) -> int: ...
    @property
    def current_temperature(self) -> float: ...
    @property
    def filter_indicator(self) -> bool: ...
    @property
    def defrosting(self) -> bool: ...
    @property
    def error_code(self) -> int: ...


def _read(appliance: DehumidifierState, attr: str) -> Any:
    """Read one attribute, returning None when it is unavailable.

    The library raises or returns None for fields a given model does not
    support, and for everything before the first successful refresh.
    """
    try:
        return getattr(appliance, attr)
    except Exception as exc:
        logger.debug("appliance field %s unavailable: %s", attr, exc)
        return None


def normalize_state(appliance: DehumidifierState) -> dict[str, Any]:
    """Flat control-state payload — everything a KNX status GA mirrors."""
    state: dict[str, Any] = {}

    for key, attr in (
        ("power", "running"),
        ("ion", "ion_mode"),
        ("sleep", "sleep_mode"),
        ("pump", "pump"),
        ("tank_full", "tank_full"),
        ("filter_indicator", "filter_indicator"),
        ("defrosting", "defrosting"),
    ):
        value = _read(appliance, attr)
        if value is not None:
            state[key] = bool(value)

    for key, attr in (
        ("mode", "mode"),
        ("fan_speed", "fan_speed"),
        ("target_humidity", "target_humidity"),
        ("error_code", "error_code"),
    ):
        value = _read(appliance, attr)
        if value is not None:
            state[key] = int(value)

    return state


def normalize_environment(appliance: DehumidifierState) -> dict[str, Any]:
    """Flat sensor payload; unsupported or not-yet-reported sensors drop out."""
    environment: dict[str, Any] = {}

    humidity = _read(appliance, "current_humidity")
    if humidity is not None and humidity >= 0:
        environment["humidity"] = int(humidity)

    temperature = _read(appliance, "current_temperature")
    if temperature is not None:
        environment["temperature_c"] = round(float(temperature), 1)

    tank_level = _read(appliance, "tank_level")
    if tank_level is not None and tank_level >= 0:
        environment["tank_level"] = int(tank_level)

    return environment
