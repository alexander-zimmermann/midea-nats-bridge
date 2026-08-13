"""Unit tests for Settings validation, device loading, and derived subjects."""

from __future__ import annotations

import textwrap
from pathlib import Path

import pytest
from pydantic import ValidationError

from midea_nats_bridge.config import DeviceConfig, Settings


def _settings(**overrides: object) -> Settings:
    return Settings(**overrides)  # type: ignore[arg-type]


def _devices_file(tmp_path: Path, body: str, name: str = "devices.yaml") -> Path:
    path = tmp_path / name
    path.write_text(textwrap.dedent(body))
    return path


def test_subjects_derive_from_device_name() -> None:
    device = DeviceConfig(name="vorratsraum", host="entfeuchter.local")
    assert device.state_subject == "midea.vorratsraum.state"
    assert device.environment_subject == "midea.vorratsraum.environment"


def test_command_filter_is_wildcard_across_devices() -> None:
    assert _settings().command_subject_filter == "midea.*.command.>"


def test_device_name_must_be_single_token() -> None:
    with pytest.raises(ValidationError):
        DeviceConfig(name="kg.vorratsraum", host="entfeuchter.local")


def test_device_rejects_unknown_keys() -> None:
    with pytest.raises(ValidationError):
        DeviceConfig(name="x", host="a.local", typo="oops")  # type: ignore[call-arg]


def test_load_devices_stamps_subject_prefix(tmp_path: Path) -> None:
    path = _devices_file(
        tmp_path,
        """
        devices:
          - name: hauswirtschaftsraum
            host: entfeuchter-hwr.local
          - name: vorratsraum
            host: entfeuchter-vorratsraum.local
        """,
    )
    devices = _settings(midea_devices_file=path, nats_subject_prefix="klima").load_devices()

    assert [d.name for d in devices] == ["hauswirtschaftsraum", "vorratsraum"]
    assert devices[0].state_subject == "klima.hauswirtschaftsraum.state"


def test_load_devices_rejects_duplicate_names(tmp_path: Path) -> None:
    path = _devices_file(
        tmp_path,
        """
        devices:
          - {name: same, host: a.local}
          - {name: same, host: b.local}
        """,
    )
    with pytest.raises(RuntimeError, match="duplicate device names"):
        _settings(midea_devices_file=path).load_devices()


def test_load_devices_rejects_empty_and_malformed(tmp_path: Path) -> None:
    empty = _devices_file(tmp_path, "devices: []\n", name="empty.yaml")
    with pytest.raises(RuntimeError, match="declares no devices"):
        _settings(midea_devices_file=empty).load_devices()

    malformed = _devices_file(tmp_path, "appliances:\n  - name: x\n", name="malformed.yaml")
    with pytest.raises(RuntimeError, match="top-level 'devices' list"):
        _settings(midea_devices_file=malformed).load_devices()

    scalar = _devices_file(tmp_path, "devices:\n  - vorratsraum\n", name="scalar.yaml")
    with pytest.raises(RuntimeError, match="must be a mapping"):
        _settings(midea_devices_file=scalar).load_devices()

    with pytest.raises(RuntimeError, match="does not exist"):
        _settings(midea_devices_file=tmp_path / "missing.yaml").load_devices()


def test_poll_interval_must_be_positive() -> None:
    with pytest.raises(ValidationError):
        _settings(poll_interval=0)


def test_read_device_credentials(tmp_path: Path) -> None:
    (tmp_path / "vorratsraum.token").write_text("T0KEN\n")
    (tmp_path / "vorratsraum.key").write_text("K3Y\n")
    (tmp_path / "half.token").write_text("T0KEN\n")
    (tmp_path / "empty.token").write_text("")
    (tmp_path / "empty.key").write_text("K3Y\n")
    settings = _settings(midea_credentials_dir=tmp_path)

    creds = settings.read_device_credentials("vorratsraum")
    assert (creds.token, creds.key) == ("T0KEN", "K3Y")

    # A half-provisioned device must fail loudly, not connect with a blank key.
    with pytest.raises(RuntimeError, match="does not exist"):
        settings.read_device_credentials("half")
    with pytest.raises(RuntimeError, match="is empty"):
        settings.read_device_credentials("empty")
    with pytest.raises(RuntimeError, match="does not exist"):
        settings.read_device_credentials("missing")


def test_nats_auth_precedence(tmp_path: Path) -> None:
    seed = tmp_path / "nkey-seed"
    seed.write_text("SUAB...")
    password = tmp_path / "nats-password"
    password.write_text("pw\n")

    assert _settings().nats_auth_kwargs() == {}
    assert _settings(nats_nkey_seed_file=seed).nats_auth_kwargs() == {"nkeys_seed": str(seed)}
    assert _settings(nats_user="midea", nats_user_password_file=password).nats_auth_kwargs() == {
        "user": "midea",
        "password": "pw",
    }
    # nkey seed wins over user/password.
    assert _settings(
        nats_nkey_seed_file=seed, nats_user="midea", nats_user_password_file=password
    ).nats_auth_kwargs() == {"nkeys_seed": str(seed)}
