# midea-nats-bridge

Bridge Comfee/Midea dehumidifiers to NATS JetStream. The appliances speak a
proprietary protocol on the local network (TCP 6444, encrypted); this service
talks to them via
[midea-beautiful-air](https://github.com/nbogojevic/midea-beautiful-air),
normalizes the dialect into flat scalar JSON, and publishes to NATS. Commands
flow the other way on core NATS subjects.

```
Comfee dehumidifiers (LAN :6444)
  ↕ midea-nats-bridge                       # one process, N appliances
    → midea.<device>.state        {"power": true, "mode": 1, "fan_speed": 40,
                                   "target_humidity": 55, "tank_full": false, ...}
    → midea.<device>.environment  {"humidity": 62, "temperature_c": 21.4, "tank_level": 30}
    ← midea.<device>.command.{power,mode,fan_speed,target_humidity,lock}  {"value": ...}
```

Design notes:

- One process serves any number of appliances: a device list in YAML, one
  connection and poll loop per appliance, and a single wildcard command
  subscription (`midea.*.command.>`) routed by the device token in the subject.
  Every device-scoped metric carries a `device` label.
- **Polling is the only source of state.** The Midea LAN protocol has no push
  channel, unlike Dyson where polling merely backstops the device's own pushes.
  `POLL_INTERVAL` (default 60 s) therefore sets the end-to-end latency.
- Unchanged payloads are not republished. The appliance reports the same values
  every poll, and forwarding that would only add noise to NATS and to the KNX
  writer's change detection.
- `command.lock` is not an appliance capability. It is a bridge-side flag
  (`true` = locked) that makes the other commands no-ops, so a KNX/Basalte lock
  can hold an appliance at its current setting. Unlocking always gets through, a
  swallowed command produces no status write, and the flag is restored from the
  last archived state message on startup so a restart cannot silently unlock.
- A failed poll drops the connection handle so the supervisor reconnects, rather
  than polling a dead socket forever.
- `/healthz` covers NATS and the logging pipeline only. A dehumidifier on a
  switched socket may legitimately be powerless, so it surfaces as
  `midea_connected{device=…} 0` instead of restart-looping the pod. A missing
  credential file, by contrast, fails startup — that is misconfiguration.

## mode and fan speed value sets

`mode` and `fan_speed` are passed through as the appliance's own integers,
deliberately untranslated. The library accepts `mode` 0-15 and `fan_speed`
0-127, but those are **library** bounds — a given dehumidifier answers to a
handful of values, and a wrong guess is invisible: the appliance simply does
something other than the KNX label promises.

Observed on an MDDF unit (capabilities `{"auto": 1, "dry_clothes": 1,
"fan_speed": 7}`) by switching it and reading back:

| Field       | Confirmed values | Notes                                                               |
| ----------- | ---------------- | ------------------------------------------------------------------- |
| `mode`      | 1, 3             | 1 while targeting a set humidity; 3 reached via the mode button     |
| `fan_speed` | 60, 80           | a third, lower step is likely — `fan_speed: 7` reads as three steps |

Incomplete on purpose rather than filled in by inference. Nothing depends on
it: the KNX group addresses use DPT 5.010 (0..255), so raw values carry either
way, and once the bridge runs every value the appliance reports lands in NATS
and TimescaleDB. Pin the remaining rungs from that history rather than from a
guess.

## Devices

Non-secret details live in a YAML file (`MIDEA_DEVICES_FILE`), the token/key
pair per appliance in `MIDEA_CREDENTIALS_DIR/<name>.token` and `<name>.key`.
`name` is the subject slug (`midea.<name>.state`), decoupled from the appliance
id so a device can be swapped without breaking consumers.

```yaml
devices:
  - name: hauswirtschaftsraum
    host: entfeuchter-hwr.example.com
  - name: vorratsraum
    host: entfeuchter-vorratsraum.example.com
```

Duplicate names, an empty list, and unknown keys are rejected at startup.

## One-time credential bootstrap

V3 appliances only hand out their local token/key via the Midea cloud. Fetch
them once — locally, never in the cluster — then everything runs LAN-only:

```sh
uv run midea-cloud-fetch --account you@example.com --password '…' --app "NetHome Plus"
```

There is no "Comfee" app on the Midea side — Comfee appliances are registered in
one of the Midea apps, usually **NetHome Plus** for older units and
**MSmartHome** for newer ones. Which one holds a given appliance depends on
where it was onboarded; if one returns nothing, try the other.

This prints each appliance's id, address, token and key, plus the capabilities
dump and a full state readout. Against an appliance you already have
credentials for:

```sh
uv run midea-cloud-fetch --host 192.0.2.10 --token … --key …
```

## Configuration (env)

| Variable                | Default                               | Description                                    |
| ----------------------- | ------------------------------------- | ---------------------------------------------- |
| `MIDEA_DEVICES_FILE`    | `/etc/midea-nats-bridge/devices.yaml` | Device list (see above)                        |
| `MIDEA_CREDENTIALS_DIR` | `/etc/midea-nats-bridge/credentials`  | `<name>.token` and `<name>.key` per device     |
| `POLL_INTERVAL`         | `60`                                  | Seconds between polls, per appliance           |
| `NATS_SERVERS`          | `nats://localhost:4222`               | Comma-separated server list                    |
| `NATS_NKEY_SEED_FILE`   | —                                     | NKey seed (or creds file / user+password file) |
| `NATS_STREAM_NAME`      | `MIDEA`                               | JetStream stream expected to cover `midea.>`   |
| `METRICS_PORT`          | `9090`                                | `/metrics` + `/healthz`                        |

## Metrics

Beyond the usual connection and publish counters:

| Metric                           | Purpose                                                             |
| -------------------------------- | ------------------------------------------------------------------- |
| `midea_connected{device}`        | 1 while the appliance answers                                       |
| `midea_tank_full{device}`        | 1 when the tank is full — dehumidification stops silently otherwise |
| `midea_humidity_percent{device}` | Measured relative humidity                                          |

## Development

```sh
uv sync --extra dev
uv run ruff check .
uv run mypy src
uv run pytest
```

## License

Dual licensed: MIT or GPL-2.0-or-later, at your option.
