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
  every poll, and forwarding that would only add noise for consumers that react
  to change.
- `command.lock` is not an appliance capability. It is a bridge-side flag
  (`true` = locked) that makes the other commands no-ops, so an upstream
  controller can hold an appliance at its current setting. Unlocking gets through, a
  swallowed command produces no status write, and the flag is restored from the
  last archived state message on startup so a restart cannot silently unlock.
- A failed poll drops the connection handle so the supervisor reconnects, rather
  than polling a dead socket forever.
- **Commands are confirmed, not assumed.** `apply()` is fire-and-forget: the
  appliance acknowledges nothing and silently refuses commands it cannot honour
  — a power change inside the compressor's restart lockout, for instance. The
  bridge therefore re-reads the appliance after each command and compares
  against what was asked, backing off over `COMMAND_CONFIRM_DELAYS` (default
  `1,2,4,8,16` seconds) and settling on the first agreement. How long a unit
  takes to carry a command into its own state is not specified anywhere and
  varies with what it was doing, so backing off beats betting on one interval.
  Only the last attempt may declare a mismatch — a warning plus
  `midea_command_confirmations_total{outcome="mismatch"}`.

  Confirming also publishes state, so a status consumer usually sees the result
  about a second after the command instead of waiting out `POLL_INTERVAL`.
  Intermediate disagreements publish nothing: writing the superseded value to a
  status address would make the group address flap on its way to the right
  answer. A newer command for the same function supersedes the pending check
  rather than being reported against it — what happens when an upstream
  controller re-asserts inside the window.

- **A command the appliance never received is held, not dropped.** These units
  drop off the WLAN regularly, and an upstream controller reacting to a room
  sensor only announces *changes* — so a command lost while the appliance was
  unreachable stays lost until the next edge, which can be hours. The bridge
  therefore keeps the last undelivered value per function as the desired state
  and re-sends it on the next successful connect, until it is delivered or a
  newer command replaces it. `midea_pending_commands` shows what is waiting,
  `midea_command_reasserts_total` how often it had to be re-sent, and
  `midea_commands_total{outcome="deferred"}` how often a command could not go
  straight through. A locked appliance drops what is held rather than firing it
  late. Nothing survives a pod restart; the desired state lives in memory.

  Delivery is not the same as agreement — a command that reached the appliance
  and was ignored is a `mismatch` above, not something to re-send.
- Every appliance round-trip is serialised per device. `refresh()` overwrites
  the library's `state` object wholesale, so an overlapping refresh would
  discard the attribute set for a pending `apply()` and write back the old value.
- `/healthz` covers NATS and the logging pipeline only. A dehumidifier on a
  switched socket may legitimately be powerless, so it surfaces as
  `midea_connected{device=…} 0` instead of restart-looping the pod. A missing
  credential file, by contrast, fails startup — that is misconfiguration.

## mode and fan speed value sets

`mode` and `fan_speed` are passed through as the appliance's own integers,
deliberately untranslated. The library accepts `mode` 0-15 and `fan_speed`
0-127, but those are **library** bounds — a given dehumidifier answers to a
handful of values, and a wrong guess is invisible: the appliance simply does
something other than the label in front of it promises.

Observed on an MDDF unit (capabilities `{"auto": 1, "dry_clothes": 1,
"fan_speed": 7}`) by switching it and reading back:

| `mode` | Button pressed                                               |
| ------ | ------------------------------------------------------------ |
| 1      | no mode selected — also the state the appliance powers on in |
| 2      | Dry clothes — the `dry_clothes` capability                   |
| 3      | Continuous                                                   |
| 4      | Smart — the `auto` capability                                |

| `fan_speed`  |                                                                    |
| ------------ | ------------------------------------------------------------------ |
| 40 / 60 / 80 | low / medium / high — three steps, matching `fan_speed: 7` (0b111) |

Each identified value came from pressing that button and reading the result
back. Note what _not_ to conclude: pressing Smart once left `mode` at 1 and it
was briefly recorded as meaning Smart, but the press had simply not registered.
An absent change is not evidence of a match — only an observed transition is.

In Smart the appliance drives `fan_speed` itself; it jumped 40 → 80 with no
one touching the fan, and kept 80 after leaving Smart rather than restoring the
previous step. A speed control offered during Smart fights the appliance's own
regulation, and a control elsewhere shows a value the appliance last chose for
itself.

`target_humidity` survives a mode change but only acts in Smart, so a setpoint
shown during continuous operation displays a number with no effect.

A different model answers to a different set. Derive it the same way rather
than reusing this table.

One caution when reading values back by hand: an appliance object that has not
been read returns the library's defaults — `mode 0`, `fan_speed 40`,
`target_humidity 50`, `current_humidity 45`, `current_temperature 0`. Those look
exactly like readings. Check that humidity and temperature differ from 45 and 0
before trusting a dump.

`LanDevice.online` does not rule it out. That flag is about the network
exchange: the library raises it the moment an appliance answers discovery, with
the state still at those defaults, and leaves it up while a refresh quietly
returns nothing. Only the `Appliance` object's own `online` — set when a status
response is parsed — means the values are real, which is why the bridge requires
both before publishing.

## Devices

Non-secret details live in a YAML file (`MIDEA_DEVICES_FILE`), the token/key
pair per appliance in `MIDEA_CREDENTIALS_DIR/<name>.token` and `<name>.key`.
`name` is the subject slug (`midea.<name>.state`), decoupled from the appliance
id so a device can be swapped without breaking consumers. Naming it after the
host keeps a device called the same thing in DNS, in NATS and in the Secret.

```yaml
devices:
  - name: basement
    host: dehumidifier-basement.example.com
  - name: cellar
    host: dehumidifier-cellar.example.com
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

| Variable                 | Default                               | Description                                                     |
| ------------------------ | ------------------------------------- | --------------------------------------------------------------- |
| `MIDEA_DEVICES_FILE`     | `/etc/midea-nats-bridge/devices.yaml` | Device list (see above)                                         |
| `MIDEA_CREDENTIALS_DIR`  | `/etc/midea-nats-bridge/credentials`  | `<name>.token` and `<name>.key` per device                      |
| `POLL_INTERVAL`          | `60`                                  | Seconds between polls, per appliance                            |
| `COMMAND_CONFIRM_DELAYS` | `1,2,4,8,16`                          | Waits between post-command re-reads, in seconds; empty disables |
| `NATS_SERVERS`           | `nats://localhost:4222`               | Comma-separated server list                                     |
| `NATS_NKEY_SEED_FILE`    | —                                     | NKey seed (or creds file / user+password file)                  |
| `NATS_STREAM_NAME`       | `MIDEA`                               | JetStream stream expected to cover `midea.>`                    |
| `METRICS_PORT`           | `9090`                                | `/metrics` + `/healthz`                                         |

## Metrics

Beyond the usual connection and publish counters:

| Metric                                                       | Purpose                                                                                               |
| ------------------------------------------------------------ | ----------------------------------------------------------------------------------------------------- |
| `midea_connected{device}`                                    | 1 while the appliance answers                                                                         |
| `midea_tank_full{device}`                                    | 1 when the tank is full — dehumidification stops silently otherwise                                   |
| `midea_humidity_percent{device}`                             | Measured relative humidity                                                                            |
| `midea_command_confirmations_total{device,function,outcome}` | Post-command re-read: `confirmed`, `mismatch` (the appliance refused it), `superseded`, `unavailable` |

## Development

```sh
uv sync --extra dev
uv run ruff check .
uv run mypy src
uv run pytest
```

## License

Dual licensed: MIT or GPL-2.0-or-later, at your option.
