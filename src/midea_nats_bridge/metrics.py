"""Prometheus metrics registry and a tiny HTTP server exposing /metrics and /healthz."""

from __future__ import annotations

import asyncio
import contextlib
import logging
from collections.abc import Awaitable, Callable

from prometheus_client import (
    CONTENT_TYPE_LATEST,
    CollectorRegistry,
    Counter,
    Gauge,
    generate_latest,
)

from .logging_setup import TrackedStreamHandler

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
            "Commands received on NATS by function and outcome (ok | invalid | error)",
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


async def serve(
    metrics: Metrics,
    port: int,
    is_healthy: Callable[[], Awaitable[bool]] | Callable[[], bool],
) -> asyncio.AbstractServer:
    """Start a tiny HTTP server exposing /metrics and /healthz."""

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                writer.close()
                return
            # Drain the rest of the request headers.
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break

            parts = request_line.decode("ascii", errors="replace").split()
            path = parts[1] if len(parts) >= 2 else "/"

            # The handler serves exactly one response per connection, so
            # announce that instead of HTTP/1.1's implicit keep-alive.
            if path.startswith("/metrics"):
                body = generate_latest(metrics.registry)
                writer.write(
                    b"HTTP/1.1 200 OK\r\n"
                    b"Connection: close\r\n"
                    + f"Content-Type: {CONTENT_TYPE_LATEST}\r\n".encode("ascii")
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            elif path.startswith("/healthz"):
                result = is_healthy()
                if asyncio.iscoroutine(result):
                    ok = await result
                else:
                    ok = bool(result)
                status = b"200 OK" if ok else b"503 Service Unavailable"
                body = b"ok\n" if ok else b"unhealthy\n"
                writer.write(
                    b"HTTP/1.1 " + status + b"\r\n"
                    b"Connection: close\r\n"
                    b"Content-Type: text/plain\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            else:
                body = b"not found\n"
                writer.write(
                    b"HTTP/1.1 404 Not Found\r\n"
                    b"Connection: close\r\n"
                    b"Content-Type: text/plain\r\n"
                    + f"Content-Length: {len(body)}\r\n\r\n".encode("ascii")
                    + body
                )
            await writer.drain()
        except Exception:
            logger.exception("metrics http handler failed")
        finally:
            writer.close()
            with contextlib.suppress(Exception):
                await writer.wait_closed()

    server = await asyncio.start_server(handle, host="0.0.0.0", port=port)
    logger.info("metrics server listening on :%d", port)
    return server
