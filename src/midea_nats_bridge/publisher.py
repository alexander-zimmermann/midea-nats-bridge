"""NATS connection owner: JetStream publish with retry, plus core subscriptions."""

from __future__ import annotations

import asyncio
import contextlib
import json
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from nats.aio.client import Client as NatsClient
from nats.aio.msg import Msg
from nats.errors import NoRespondersError
from nats.errors import TimeoutError as NATSTimeoutError
from nats.js import JetStreamContext
from nats.js.errors import APIError, NoStreamResponseError

from .config import Settings
from .metrics import Metrics

logger = logging.getLogger(__name__)

# Upper bound on queued-but-unpublished messages. The device emits at most a
# few messages per poll interval, so this covers hours of NATS outage.
_PUBLISH_QUEUE_MAX = 1000

# How long close() waits for the queue to drain before cancelling the worker.
_CLOSE_FLUSH_TIMEOUT_SECONDS = 5.0


class Publisher:
    """Single NATS client shared by every device bridge and the command subscriber.

    Messages enter through the synchronous enqueue(); a single worker task
    drains the queue, so messages are published in arrival order.
    """

    def __init__(self, settings: Settings, metrics: Metrics) -> None:
        self._settings = settings
        self._metrics = metrics
        self._nc: NatsClient | None = None
        self._js: JetStreamContext | None = None
        self._queue: asyncio.Queue[tuple[str, str, str, dict[str, Any]]] = asyncio.Queue(
            maxsize=_PUBLISH_QUEUE_MAX
        )
        self._worker: asyncio.Task[None] | None = None

    async def connect(self) -> None:
        if self._nc and self._nc.is_connected:
            return

        kwargs = self._settings.nats_auth_kwargs()
        kwargs.update(
            servers=self._settings.nats_servers_list,
            max_reconnect_attempts=-1,
            reconnect_time_wait=2,
            connect_timeout=10,
            disconnected_cb=self._on_disconnect,
            reconnected_cb=self._on_reconnect,
            closed_cb=self._on_closed,
            error_cb=self._on_error,
        )

        self._nc = NatsClient()
        await self._nc.connect(**kwargs)
        self._js = self._nc.jetstream()
        self._metrics.nats_connected.set(1)
        logger.info("connected to NATS: %s", self._settings.nats_servers_list)

        if self._settings.nats_stream_check:
            await self._verify_stream()

        if self._worker is None:
            self._worker = asyncio.create_task(self._drain_queue())

    async def _verify_stream(self) -> None:
        assert self._js is not None
        try:
            info = await self._js.stream_info(self._settings.nats_stream_name)
            logger.info(
                "jetstream stream ok: %s (subjects=%s, messages=%d)",
                info.config.name,
                info.config.subjects,
                info.state.messages,
            )
        except Exception as exc:
            logger.warning(
                "jetstream stream %r not reachable at startup: %s",
                self._settings.nats_stream_name,
                exc,
            )

    async def last_message(self, subject: str) -> dict[str, Any] | None:
        """Last archived message on `subject`, or None when there is none.

        Used to restore state that only ever arrives over NATS (the lock flag),
        so a pod restart doesn't silently drop it.
        """
        if self._js is None:
            return None
        try:
            msg = await self._js.get_last_msg(self._settings.nats_stream_name, subject)
        except Exception as exc:
            logger.info("no archived message for %s: %s", subject, exc)
            return None

        try:
            payload = json.loads(msg.data or b"")
        except json.JSONDecodeError:
            logger.warning("archived message on %s is not valid JSON", subject)
            return None
        return payload if isinstance(payload, dict) else None

    async def subscribe_core(
        self, subject: str, callback: Callable[[Msg], Awaitable[None]]
    ) -> None:
        """Core (non-JetStream) subscription, used for the command subjects."""
        if self._nc is None:
            raise RuntimeError("subscribe_core() before connect()")
        await self._nc.subscribe(subject, cb=callback)
        logger.info("subscribed to %s", subject)

    async def close(self) -> None:
        if self._worker is not None:
            # Best-effort flush so a clean shutdown doesn't drop queued messages.
            with contextlib.suppress(TimeoutError):
                await asyncio.wait_for(self._queue.join(), timeout=_CLOSE_FLUSH_TIMEOUT_SECONDS)
            self._worker.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
        if self._nc and self._nc.is_connected:
            await self._nc.drain()
        self._metrics.nats_connected.set(0)

    @property
    def is_connected(self) -> bool:
        return bool(self._nc and self._nc.is_connected)

    def enqueue(self, device: str, kind: str, subject: str, payload: dict[str, Any]) -> bool:
        """Queue one message for publishing; safe to call from the event loop only.

        Returns False (and counts a queue_full error) when the buffer is full,
        e.g. during a prolonged NATS outage.
        """
        try:
            self._queue.put_nowait((device, kind, subject, payload))
        except asyncio.QueueFull:
            self._metrics.publish_errors.labels(device=device, reason="queue_full").inc()
            logger.warning(
                "publish queue full (%d), dropping message for %s", self._queue.maxsize, subject
            )
            return False
        return True

    async def _drain_queue(self) -> None:
        while True:
            device, kind, subject, payload = await self._queue.get()
            try:
                await self.publish(device, kind, subject, payload)
            except Exception:
                logger.exception("unexpected error publishing %s", subject)
            finally:
                self._queue.task_done()

    async def publish(self, device: str, kind: str, subject: str, payload: dict[str, Any]) -> bool:
        """Publish one message, waiting for a JetStream ack.

        Returns True on success, False on a permanent failure after retries.
        """
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode("utf-8")

        backoff = 0.1
        for attempt in range(1, 4):
            if not self._js:
                self._metrics.publish_errors.labels(device=device, reason="other").inc()
                return False
            try:
                await self._js.publish(subject, body, timeout=5.0)
                self._metrics.messages_published.labels(device=device, kind=kind).inc()
                return True
            except NoStreamResponseError:
                # Stream/subject misconfiguration: retrying won't help, and any
                # sleep here would stall the ordered publish queue.
                self._metrics.publish_errors.labels(device=device, reason="no_stream").inc()
                logger.error("no stream matches subject %s (attempt %d)", subject, attempt)
                return False
            except NATSTimeoutError:
                self._metrics.publish_errors.labels(device=device, reason="timeout").inc()
                logger.warning("publish timeout for %s (attempt %d)", subject, attempt)
            except NoRespondersError:
                self._metrics.publish_errors.labels(device=device, reason="nak").inc()
                logger.warning("no responders for %s (attempt %d)", subject, attempt)
            except APIError as exc:
                self._metrics.publish_errors.labels(device=device, reason="nak").inc()
                logger.warning("jetstream api error for %s (attempt %d): %s", subject, attempt, exc)
            except Exception:
                self._metrics.publish_errors.labels(device=device, reason="other").inc()
                logger.exception("unexpected publish error for %s (attempt %d)", subject, attempt)

            if attempt < 3:
                await asyncio.sleep(backoff)
                backoff *= 2

        return False

    async def _on_disconnect(self) -> None:
        self._metrics.nats_connected.set(0)
        logger.warning("nats disconnected")

    async def _on_reconnect(self) -> None:
        self._metrics.nats_connected.set(1)
        logger.info("nats reconnected")

    async def _on_closed(self) -> None:
        self._metrics.nats_connected.set(0)
        logger.warning("nats client closed")

    async def _on_error(self, err: Exception) -> None:
        logger.warning("nats error callback: %s", err)
