"""Settings from env vars (pydantic-settings); devices from YAML, secrets from files."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import yaml
from nats_bridge_core import NatsSettings
from pydantic import BaseModel, ConfigDict, field_validator


class DeviceConfig(BaseModel):
    """One dehumidifier: connection details plus its NATS subject namespace."""

    model_config = ConfigDict(frozen=True, extra="forbid")

    # Stable slug used in NATS subjects (midea.<name>.state), decoupled from the
    # appliance id so a device can be swapped without breaking consumers.
    name: str
    host: str
    subject_prefix: str = "midea"

    @field_validator("name", "subject_prefix")
    @classmethod
    def _single_token(cls, v: str) -> str:
        if "." in v or "/" in v or " " in v or not v:
            raise ValueError("must be a non-empty single token (no dots, slashes, spaces)")
        return v

    @property
    def state_subject(self) -> str:
        return f"{self.subject_prefix}.{self.name}.state"

    @property
    def environment_subject(self) -> str:
        return f"{self.subject_prefix}.{self.name}.environment"

    @property
    def availability_subject(self) -> str:
        return f"{self.subject_prefix}.{self.name}.availability"


class Credentials(BaseModel):
    """The V3 token/key pair, obtained once from the cloud and reused locally."""

    model_config = ConfigDict(frozen=True)

    token: str
    key: str


class Settings(NatsSettings):
    # Devices: non-secret details in a YAML file (ConfigMap), the token/key pair
    # per device as <credentials_dir>/<name>.{token,key} (Secret).
    midea_devices_file: Path = Path("/etc/midea-nats-bridge/devices.yaml")
    midea_credentials_dir: Path = Path("/etc/midea-nats-bridge/credentials")
    # Seconds between polls. The Midea LAN protocol has no push channel, so this
    # is the only source of state — unlike Dyson, where polling merely backstops.
    poll_interval: float = 60.0
    # Waits between re-reads after a command, in seconds, until the appliance
    # agrees. Backing off rather than guessing one delay: how long a unit takes
    # to carry a command into its own state is not specified anywhere and varies
    # with what it was doing. Empty disables confirmation.
    command_confirm_delays: str = "1,2,4,8,16"

    # NATS
    nats_subject_prefix: str = "midea"
    nats_stream_name: str = "MIDEA"

    @property
    def command_subject_filter(self) -> str:
        """One wildcard subscription covers every device."""
        return f"{self.nats_subject_prefix}.*.command.>"

    @property
    def command_confirm_delays_list(self) -> list[float]:
        return [float(p) for p in self.command_confirm_delays.split(",") if p.strip()]

    @field_validator("poll_interval")
    @classmethod
    def _poll_interval_positive(cls, v: float) -> float:
        if v <= 0:
            raise ValueError("POLL_INTERVAL must be > 0 seconds")
        return v

    @field_validator("command_confirm_delays")
    @classmethod
    def _confirm_delays_valid(cls, v: str) -> str:
        for part in v.split(","):
            if not part.strip():
                continue
            try:
                seconds = float(part)
            except ValueError as exc:
                raise ValueError(
                    f"COMMAND_CONFIRM_DELAYS must be comma-separated numbers, got {part!r}"
                ) from exc
            if seconds < 0:
                raise ValueError("COMMAND_CONFIRM_DELAYS entries must be >= 0 seconds")
        return v

    def load_devices(self) -> list[DeviceConfig]:
        """Parse the devices YAML; raises on an empty list or duplicate names."""
        if not self.midea_devices_file.exists():
            raise RuntimeError(f"MIDEA_DEVICES_FILE {self.midea_devices_file} does not exist")
        data: Any = yaml.safe_load(self.midea_devices_file.read_text()) or {}
        if not isinstance(data, dict) or not isinstance(data.get("devices"), list):
            raise RuntimeError(f"{self.midea_devices_file} must contain a top-level 'devices' list")

        devices: list[DeviceConfig] = []
        for entry in data["devices"]:
            if not isinstance(entry, dict):
                raise RuntimeError(f"{self.midea_devices_file}: each device must be a mapping")
            devices.append(DeviceConfig(**{**entry, "subject_prefix": self.nats_subject_prefix}))
        if not devices:
            raise RuntimeError(f"{self.midea_devices_file} declares no devices")

        names = [d.name for d in devices]
        duplicates = sorted({n for n in names if names.count(n) > 1})
        if duplicates:
            raise RuntimeError(f"duplicate device names in {self.midea_devices_file}: {duplicates}")
        return devices

    def read_device_credentials(self, device_name: str) -> Credentials:
        """Read the token/key pair for one device; both files must be present."""
        values: dict[str, str] = {}
        for part in ("token", "key"):
            path = self.midea_credentials_dir / f"{device_name}.{part}"
            if not path.exists():
                raise RuntimeError(
                    f"credential file {path} for device {device_name!r} does not exist"
                )
            value = path.read_text().strip()
            if not value:
                raise RuntimeError(f"credential file {path} for device {device_name!r} is empty")
            values[part] = value
        return Credentials(token=values["token"], key=values["key"])
