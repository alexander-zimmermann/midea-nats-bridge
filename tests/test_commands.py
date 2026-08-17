"""Unit tests for command parsing and per-appliance dispatch."""

from __future__ import annotations

import json
from typing import Any

import pytest

from midea_nats_bridge.commands import CommandHandler, parse_command, split_subject
from midea_nats_bridge.config import Settings
from midea_nats_bridge.metrics import Metrics


def _payload(value: Any) -> bytes:
    return json.dumps({"value": value}).encode()


def test_split_subject() -> None:
    assert split_subject("midea.vorratsraum.command.power") == ("vorratsraum", "power")
    for bad in ("midea.x.power", "midea.x.state.power", "midea.x.command.power.extra"):
        with pytest.raises(ValueError, match="malformed command subject"):
            split_subject(bad)


def test_parse_switches() -> None:
    assert parse_command("midea.x.command.power", _payload(True)) == ("x", "power", True)
    assert parse_command("midea.x.command.lock", _payload(0)) == ("x", "lock", False)


def test_parse_integer_ranges() -> None:
    assert parse_command("midea.x.command.mode", _payload(1)) == ("x", "mode", 1)
    assert parse_command("midea.x.command.fan_speed", _payload(127)) == ("x", "fan_speed", 127)
    assert parse_command("midea.x.command.target_humidity", _payload(55)) == (
        "x",
        "target_humidity",
        55,
    )
    with pytest.raises(ValueError, match="0..15"):
        parse_command("midea.x.command.mode", _payload(16))
    with pytest.raises(ValueError, match="0..100"):
        parse_command("midea.x.command.target_humidity", _payload(101))


def test_parse_rejects_bool_for_integers() -> None:
    # True would silently become 1 and look like a valid mode.
    with pytest.raises(ValueError, match="must be a number"):
        parse_command("midea.x.command.mode", _payload(True))


def test_parse_rejects_unknown_function_and_bad_payloads() -> None:
    with pytest.raises(ValueError, match="unknown command function"):
        parse_command("midea.x.command.warp", _payload(1))
    with pytest.raises(ValueError, match="invalid JSON"):
        parse_command("midea.x.command.power", b"{nope")
    with pytest.raises(ValueError, match='"value" field'):
        parse_command("midea.x.command.power", b'{"on": true}')


class FakeMsg:
    def __init__(self, subject: str, data: bytes) -> None:
        self.subject = subject
        self.data = data


class FakeBridge:
    def __init__(self, fail: bool = False, locked: bool = False) -> None:
        self.fail = fail
        self.locked = locked
        self.applied: list[tuple[str, Any]] = []

    async def apply_command(self, function: str, value: Any) -> None:
        if self.fail:
            raise RuntimeError("appliance offline")
        self.applied.append((function, value))

    def set_lock(self, locked: bool) -> None:
        self.locked = locked


def _handler(
    metrics: Metrics, **bridges: FakeBridge
) -> tuple[CommandHandler, dict[str, FakeBridge]]:
    handler = CommandHandler(
        Settings(),
        bridges,  # type: ignore[arg-type]
        publisher=None,  # type: ignore[arg-type]
        metrics=metrics,
    )
    return handler, bridges


def _counter_value(metrics: Metrics, device: str, function: str, outcome: str) -> float:
    counter = metrics.commands.labels(device=device, function=function, outcome=outcome)
    return float(counter._value.get())


async def test_handler_routes_to_the_addressed_appliance() -> None:
    metrics = Metrics()
    handler, bridges = _handler(metrics, vorratsraum=FakeBridge(), hwr=FakeBridge())

    await handler._on_command(FakeMsg("midea.hwr.command.target_humidity", _payload(55)))  # type: ignore[arg-type]

    assert bridges["hwr"].applied == [("target_humidity", 55)]
    assert bridges["vorratsraum"].applied == []
    assert _counter_value(metrics, "hwr", "target_humidity", "ok") == 1


async def test_handler_counts_unknown_device() -> None:
    metrics = Metrics()
    handler, bridges = _handler(metrics, vorratsraum=FakeBridge())

    await handler._on_command(FakeMsg("midea.garage.command.power", _payload(True)))  # type: ignore[arg-type]

    assert bridges["vorratsraum"].applied == []
    assert _counter_value(metrics, "garage", "power", "unknown_device") == 1


async def test_lock_blocks_other_commands_but_not_unlocking() -> None:
    metrics = Metrics()
    handler, bridges = _handler(metrics, hwr=FakeBridge())
    bridge = bridges["hwr"]

    await handler._on_command(FakeMsg("midea.hwr.command.lock", _payload(True)))  # type: ignore[arg-type]
    assert bridge.locked is True

    await handler._on_command(FakeMsg("midea.hwr.command.fan_speed", _payload(60)))  # type: ignore[arg-type]
    assert bridge.applied == []
    assert _counter_value(metrics, "hwr", "fan_speed", "locked") == 1

    # Unlocking must always get through, otherwise the appliance stays stuck.
    await handler._on_command(FakeMsg("midea.hwr.command.lock", _payload(False)))  # type: ignore[arg-type]
    assert bridge.locked is False

    await handler._on_command(FakeMsg("midea.hwr.command.fan_speed", _payload(60)))  # type: ignore[arg-type]
    assert bridge.applied == [("fan_speed", 60)]


async def test_handler_counts_appliance_errors() -> None:
    metrics = Metrics()
    handler, _ = _handler(metrics, hwr=FakeBridge(fail=True))

    await handler._on_command(FakeMsg("midea.hwr.command.power", _payload(True)))  # type: ignore[arg-type]

    assert _counter_value(metrics, "hwr", "power", "error") == 1
