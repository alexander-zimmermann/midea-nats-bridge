"""Prometheus metrics registry and a tiny HTTP server exposing /metrics and /healthz."""

from __future__ import annotations

import logging
from typing import cast

from nats_bridge_core import TrackedStreamHandler
from prometheus_client import (
    CollectorRegistry,
    Counter,
    Gauge,
)

logger = logging.getLogger(__name__)


class Metrics:
    def __init__(self) -> None:
        self.registry = CollectorRegistry()

        self.midea_connected = Gauge(
            "midea_connected",
            "1 if the appliance connection is currently up, 0 otherwise",
            ["device"],
            registry=self.registry,
        )
        self.nats_connected = Gauge(
            "nats_connected",
            "1 if NATS client is currently connected, 0 otherwise",
            registry=self.registry,
        )
        # Exposed for alerting. A full tank stops dehumidification silently —
        # the appliance keeps running and nothing downstream would notice.
        self.tank_full = Gauge(
            "midea_tank_full",
            "1 if the water tank is full and the appliance has stopped, 0 otherwise",
            ["device"],
            registry=self.registry,
        )
        self.humidity = Gauge(
            "midea_humidity_percent",
            "Relative humidity measured by the appliance, in percent",
            ["device"],
            registry=self.registry,
        )
        self.messages_received = Counter(
            "midea_messages_received_total",
            "Messages received from the device by kind (state | environment)",
            ["device", "kind"],
            registry=self.registry,
        )
        self.messages_published = Counter(
            "midea_messages_published_total",
            "Normalized messages successfully published to NATS by kind",
            ["device", "kind"],
            registry=self.registry,
        )
        self.publish_errors = Counter(
            "midea_publish_errors_total",
            "Publish errors by reason",
            ["device", "reason"],
            registry=self.registry,
        )
        self.commands = Counter(
            "midea_commands_total",
            "Commands received on NATS by function and outcome "
            "(ok | deferred | locked | invalid | error | unknown_device)",
            ["device", "function", "outcome"],
            registry=self.registry,
        )
        # A command the appliance never received is held, not dropped. Both
        # metrics are the only way to see that from outside: the sender reacts
        # to changes and never repeats itself.
        self.pending_commands = Gauge(
            "midea_pending_commands",
            "Commands accepted but not yet delivered to the appliance",
            ["device"],
            registry=self.registry,
        )
        self.command_reasserts = Counter(
            "midea_command_reasserts_total",
            "Held commands re-sent after the appliance became reachable again",
            ["device", "function"],
            registry=self.registry,
        )
        # A refused command is otherwise silent: apply() is fire-and-forget and
        # the appliance acknowledges nothing.
        self.command_confirmations = Counter(
            "midea_command_confirmations_total",
            "Post-command re-reads by function and outcome "
            "(confirmed | mismatch | superseded | unavailable)",
            ["device", "function", "outcome"],
            registry=self.registry,
        )
        self.poll_errors = Counter(
            "midea_poll_errors_total",
            "Failed state/environment poll requests to the device",
            ["device"],
            registry=self.registry,
        )
        self.reconnects = Counter(
            "midea_reconnects_total",
            "Appliance reconnect attempts by outcome (ok | error)",
            ["device", "outcome"],
            registry=self.registry,
        )
        self.last_message_ts = Gauge(
            "midea_last_message_received_timestamp",
            "Unix timestamp of the last message received from the device (seconds)",
            ["device"],
            registry=self.registry,
        )
        # Surface logger-health state so a stuck stdout is visible in Prometheus,
        # not just via liveness. Source of truth is TrackedStreamHandler.
        self.log_emit_errors = Gauge(
            "midea_bridge_log_emit_errors",
            "Cumulative count of logging handler emit() failures since pod start",
            registry=self.registry,
        )
        self.log_emit_errors.set_function(lambda: float(TrackedStreamHandler.emit_errors_total))
        self.log_last_emit_ok_timestamp = Gauge(
            "midea_bridge_log_last_emit_ok_timestamp",
            "Monotonic-seconds timestamp of the last successful log emit",
            registry=self.registry,
        )
        self.log_last_emit_ok_timestamp.set_function(
            lambda: float(TrackedStreamHandler.last_emit_ok_ts)
        )

    # --- nats_bridge_core.PublisherMetrics -------------------------------
    # ctx is the (device, kind) pair handed to enqueue().

    def set_connected(self, connected: bool) -> None:
        self.nats_connected.set(1 if connected else 0)

    def count_published(self, ctx: object) -> None:
        device, kind = cast(tuple[str, str], ctx)
        self.messages_published.labels(device=device, kind=kind).inc()

    def count_error(self, ctx: object, reason: str) -> None:
        device, _ = cast(tuple[str, str], ctx)
        self.publish_errors.labels(device=device, reason=reason).inc()
