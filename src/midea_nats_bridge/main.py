"""Entry point: wire config, metrics, publisher, device bridge, and commands; handle signals."""

from __future__ import annotations

import asyncio
import contextlib
import logging
import signal
import sys
import time

from .commands import CommandHandler
from .config import Settings
from .device import MideaBridge
from .logging_setup import TrackedStreamHandler
from .logging_setup import configure as configure_logging
from .metrics import Metrics
from .metrics import serve as serve_metrics
from .publisher import Publisher

logger = logging.getLogger(__name__)

# Liveness fails after this many seconds of consecutive log-emit failures.
# Forgiving enough for a transient stdout glitch (kubelet log rotation etc.),
# tight enough that a real wedge causes a restart well within an hour.
LOG_EMIT_RECOVERY_WINDOW_SECONDS = 60.0


def logger_watchdog_ok(now: float) -> bool:
    """Return False if log emits have been failing for longer than the recovery window."""
    if TrackedStreamHandler.emit_errors_total <= 0:
        return True
    return (now - TrackedStreamHandler.last_emit_ok_ts) <= LOG_EMIT_RECOVERY_WINDOW_SECONDS


async def _amain() -> int:
    settings = Settings()
    configure_logging(settings.log_level, settings.log_format)
    logger.info("midea-nats-bridge starting")

    devices = settings.load_devices()
    logger.info("config: %d device(s), poll=%.0fs", len(devices), settings.poll_interval)
    for device in devices:
        logger.info(
            "config: device=%s host=%s state_subject=%s",
            device.name,
            device.host,
            device.state_subject,
        )

    metrics = Metrics()
    publisher = Publisher(settings, metrics)
    # Constructing a bridge reads its credential file; a missing or empty one is
    # a configuration error and should fail startup rather than run degraded.
    bridges = {d.name: MideaBridge(settings, d, publisher, metrics) for d in devices}
    commands = CommandHandler(settings, bridges, publisher, metrics)

    def is_healthy() -> bool:
        # Appliance connectivity is deliberately NOT part of liveness: a
        # dehumidifier on a switched socket may legitimately be powerless, so it
        # pages via midea_connected instead of restart-looping the pod.
        if not publisher.is_connected:
            return False
        return logger_watchdog_ok(time.monotonic())

    http_server = await serve_metrics(metrics, settings.metrics_port, is_healthy)

    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, stop_event.set)

    try:
        await publisher.connect()
        for bridge in bridges.values():
            # Before start(), so a locked device can't be driven by an early command.
            await bridge.restore_lock()
            await bridge.start()
        await commands.start()
        logger.info("bridge is up (%d device(s))", len(bridges))
        await stop_event.wait()
    except Exception:
        logger.exception("fatal error in bridge startup/run")
        return 1
    finally:
        logger.info("shutting down")
        for name, bridge in bridges.items():
            try:
                await bridge.stop()
            except Exception:
                logger.exception("[%s] error stopping device bridge", name)
        try:
            await publisher.close()
        except Exception:
            logger.exception("error closing NATS publisher")
        http_server.close()
        with contextlib.suppress(Exception):
            await http_server.wait_closed()

    return 0


def run() -> None:
    sys.exit(asyncio.run(_amain()))


if __name__ == "__main__":
    run()
