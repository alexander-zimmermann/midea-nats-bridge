"""One-time bootstrap: fetch each appliance's token/key pair and dump its capabilities.

V3 appliances only hand out their local token/key via the Midea cloud. Fetch
them once — locally, never in the cluster — and everything afterwards runs
LAN-only.

The capabilities dump matters as much as the credentials: the library accepts
mode 0-15 and fan_speed 0-127, but a given dehumidifier answers to a handful of
values. Those bounds are the library's, not the appliance's, and the difference
is invisible at runtime — a rejected value simply does nothing.
"""

from __future__ import annotations

import argparse
import json
import sys
from typing import Any

from midea_beautiful import appliance_state, find_appliances


def _print_appliance(appliance: Any, show_credentials: bool) -> None:
    print()
    print(f"name:      {getattr(appliance, 'name', '?')}")
    print(f"id:        {getattr(appliance, 'appliance_id', '?')}")
    print(f"type:      {getattr(appliance, 'type', '?')}")
    print(f"address:   {getattr(appliance, 'address', '?')}")
    if show_credentials:
        print(f"token:     {getattr(appliance, 'token', '')}")
        print(f"key:       {getattr(appliance, 'key', '')}")


def _print_capabilities(appliance: Any) -> None:
    """Dump what the appliance says it supports, so the KNX enums can be pinned."""
    state = getattr(appliance, "state", appliance)
    capabilities = getattr(state, "capabilities", None)
    print()
    print("capabilities:")
    print(json.dumps(capabilities, indent=2, sort_keys=True, default=str))
    print()
    print("current state:")
    for attr in (
        "running",
        "mode",
        "fan_speed",
        "target_humidity",
        "current_humidity",
        "current_temperature",
        "tank_full",
        "tank_level",
        "ion_mode",
        "sleep_mode",
        "pump",
        "filter_indicator",
        "defrosting",
        "error_code",
    ):
        try:
            print(f"  {attr:22s} {getattr(state, attr)!r}")
        except Exception as exc:  # noqa: BLE001 - report, don't abort the dump
            print(f"  {attr:22s} <unavailable: {exc}>")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--account", help="Midea/Comfee app account (email)")
    parser.add_argument("--password", help="Midea/Comfee app password")
    parser.add_argument(
        "--app",
        default="Comfee",
        help="Midea app the account belongs to (default: Comfee)",
    )
    parser.add_argument(
        "--address",
        nargs="+",
        help="Addresses to search, e.g. 192.0.2.10 or 192.0.2.255 (default: broadcast)",
    )
    parser.add_argument(
        "--host",
        help="Skip discovery and dump this address directly (needs --token and --key)",
    )
    parser.add_argument("--token", help="Existing token, with --host")
    parser.add_argument("--key", help="Existing key, with --host")
    args = parser.parse_args()

    if args.host:
        if not (args.token and args.key):
            parser.error("--host requires --token and --key")
        appliance = appliance_state(address=args.host, token=args.token, key=args.key)
        _print_appliance(appliance, show_credentials=False)
        _print_capabilities(appliance)
        return

    if not (args.account and args.password):
        parser.error("either --host with credentials, or --account and --password")

    appliances = find_appliances(
        account=args.account,
        password=args.password,
        appname=args.app,
        addresses=args.address,
    )
    if not appliances:
        print("no appliances found", file=sys.stderr)
        raise SystemExit(1)

    for appliance in appliances:
        _print_appliance(appliance, show_credentials=True)
        _print_capabilities(appliance)


if __name__ == "__main__":
    main()
