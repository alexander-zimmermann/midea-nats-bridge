"""The shipped knx.yaml names only subjects and fields the bridge actually publishes."""

from __future__ import annotations

from typing import Any

import pytest
from nats_bridge_core import knx_descriptor

from tests.test_device import _bridge
from tests.test_normalize import FakeState

DESCRIPTOR = knx_descriptor.load_package("midea_nats_bridge")


class ConnectedAppliance:
    """A read appliance whose state carries every reading the reference unit reports."""

    online = True
    state = FakeState(
        online=True,
        capabilities={"auto": 1, "dry_clothes": 1, "fan_speed": 7},
        running=True,
        mode=4,
        fan_speed=60,
        target_humidity=55,
        tank_full=False,
        sleep_mode=False,
        defrosting=False,
        error_code=0,
        current_humidity=62,
        current_temperature=21.4,
    )

    def refresh(self) -> None:
        pass


@pytest.fixture
async def published(monkeypatch: pytest.MonkeyPatch) -> dict[str, dict[str, Any]]:
    """Payload per subject suffix after one poll and the stop — what leaves the process."""
    bridge, _, publisher = _bridge(monkeypatch, ConnectedAppliance())
    await bridge._poll()
    await bridge.stop()
    # The publisher's kind label is the subject suffix (device.py, config.py)
    return dict(publisher.published)


@pytest.mark.parametrize("suffix", list(DESCRIPTOR.subjects))
def test_every_descriptor_field_is_published_on_its_subject(
    published: dict[str, dict[str, Any]], suffix: str
) -> None:
    assert suffix in published

    missing = [name for name in DESCRIPTOR.subjects[suffix].fields if name not in published[suffix]]
    assert missing == []
