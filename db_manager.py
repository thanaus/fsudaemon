"""
Database Manager - Asynchronous PostgreSQL operations management
"""
import json
import asyncio
import asyncpg
import time
from collections.abc import Callable, Coroutine
from typing import Any
from models import AuditPoint, S3Event
import structlog

logger = structlog.get_logger(__name__)


class DatabaseManager:
    """Asynchronous PostgreSQL database manager"""

    def __init__(self, pool: asyncpg.Pool):
        """
        Initializes the manager with a connection pool.

        Args:
            pool: asyncpg connection pool
        """
        self.pool = pool

    @classmethod
    async def create(cls, dsn: str, min_size: int = 5, max_size: int = 20) -> "DatabaseManager":
        """
        Creates a DatabaseManager with its connection pool.

        Args:
            dsn: PostgreSQL connection DSN
            min_size: Minimum pool size
            max_size: Maximum pool size

        Returns:
            DatabaseManager instance
        """
        logger.debug("creating_db_pool", min_size=min_size, max_size=max_size)

        pool = await asyncpg.create_pool(
            dsn,
            min_size=min_size,
            max_size=max_size,
            command_timeout=60,
            max_inactive_connection_lifetime=300,  # recycle connections after 5 min of inactivity
        )

        logger.debug("db_pool_created")
        return cls(pool)

    async def load_audit_points(self) -> list[AuditPoint]:
        """
        Loads all non-deleted audit points from the database.

        Returns:
            List of active audit points (not soft-deleted)
        """
        query = """
            SELECT id, bucket, prefix, description, created_at, deleted_at
            FROM audit_points
            WHERE deleted_at IS NULL
        """

        async with self.pool.acquire() as conn:
            rows = await conn.fetch(query)

        audit_points = [
            AuditPoint(
                id=row['id'],
                bucket=row['bucket'],
                prefix=row['prefix'],
                description=row['description'],
                created_at=row['created_at'],
                deleted_at=row['deleted_at']
            )
            for row in rows
        ]

        logger.debug("audit_points_loaded_from_db", count=len(audit_points))
        return audit_points

    async def insert_events_batch(self, events: list[S3Event]) -> tuple[int, float]:
        """
        Inserts a batch of S3 events with their audit point IDs.
        Uses ON CONFLICT DO NOTHING for idempotence (restart / SQS replay without duplicates).

        Args:
            events: List of S3 events to insert

        Returns:
            Tuple of (number of events in batch, time spent in DB in seconds)
        """
        if not events:
            return 0, 0.0

        n_cols = 7  # event_time, event_name, bucket, object_key, size, version_id, audit_point_ids
        placeholders = []
        args: list[object] = []
        for i, event in enumerate(events):
            base = i * n_cols
            placeholders.append(
                f"(${base+1}, ${base+2}, ${base+3}, ${base+4}, ${base+5}, ${base+6}, ${base+7})"
            )
            args.extend([
                event.event_time,
                event.event_name,
                event.bucket,
                event.object_key,
                event.size,
                event.version_id,
                event.audit_point_ids,
            ])

        query = (
            "INSERT INTO s3_events (event_time, event_name, bucket, object_key, size, version_id, audit_point_ids) "
            "VALUES " + ",".join(placeholders) + " "
            "ON CONFLICT (bucket, object_key, event_time, event_name) DO NOTHING"
        )

        async with self.pool.acquire() as conn:
            # No explicit transaction needed: INSERT ... ON CONFLICT DO NOTHING is
            # atomic as a single statement in PostgreSQL and carries an implicit transaction.
            start_time = time.perf_counter()
            await conn.execute(query, *args)
            db_time = time.perf_counter() - start_time

        total_associations = sum(len(e.audit_point_ids) for e in events)
        logger.debug(
            "events_inserted",
            count=len(events),
            total_associations=total_associations
        )

        return len(events), db_time

    async def health_check(self) -> bool:
        """
        Checks database connection health.

        Returns:
            True if connection is OK
        """
        try:
            async with self.pool.acquire() as conn:
                await conn.fetchval("SELECT 1")
            return True
        except Exception as e:
            logger.error("db_health_check_failed", error=str(e))
            return False


class AuditPointsListener:
    """
    Listens to PostgreSQL LISTEN/NOTIFY notifications for audit point changes.
    """

    def __init__(self, db_manager: DatabaseManager, on_change_callback: Callable[[], Coroutine[Any, Any, None]]):
        """
        Initializes the listener.

        Args:
            db_manager: Database manager
            on_change_callback: Async function to call on change
        """
        self.db_manager = db_manager
        self.on_change_callback = on_change_callback
        self._conn: asyncpg.Connection | None = None
        self._listening = False
        self._loop: asyncio.AbstractEventLoop | None = None
        # Keeps strong references to in-flight tasks to prevent garbage collection
        # before completion (required since Python 3.12).
        self._pending_tasks: set[asyncio.Task[None]] = set()

    async def start_listening(self) -> None:
        """
        Starts listening for notifications on 'audit_points_changed' channel.
        """
        try:
            # Capture the running loop in an async context — guaranteed to be correct
            self._loop = asyncio.get_running_loop()

            # Acquire a dedicated connection for LISTEN
            self._conn = await self.db_manager.pool.acquire()

            # Set callback for notifications
            await self._conn.add_listener('audit_points_changed', self._handle_notification)

            self._listening = True
            logger.debug("audit_points_listener_started", channel="audit_points_changed")

        except Exception as e:
            logger.error("audit_points_listener_start_failed", error=str(e))
            raise

    async def stop_listening(self) -> None:
        """
        Stops listening for notifications.
        """
        if self._conn and self._listening:
            try:
                await self._conn.remove_listener('audit_points_changed', self._handle_notification)
                await self.db_manager.pool.release(self._conn)
                self._listening = False
                self._loop = None
                logger.debug("audit_points_listener_stopped")
            except Exception as e:
                logger.error("audit_points_listener_stop_failed", error=str(e))

    def _handle_notification(self, connection, pid, channel, payload):
        """
        Synchronous handler called by asyncpg on notification.
        Schedules the async callback safely via the captured event loop.

        A strong reference to each created task is kept in `_pending_tasks` until
        completion, preventing the garbage collector from destroying the task before
        it runs (Python 3.12+ requirement).

        Args:
            connection: PostgreSQL connection
            pid: PostgreSQL backend process ID
            channel: Notification channel
            payload: JSON payload of the notification
        """
        try:
            data = json.loads(payload)
            action = data.get('action')
            audit_point_id = data.get('id')
            bucket = data.get('bucket')
            prefix = data.get('prefix')

            logger.info(
                "audit_point_change_notification",
                action=action,
                id=audit_point_id,
                bucket=bucket,
                prefix=prefix
            )

            if self._loop and self._loop.is_running() and not self._loop.is_closed():
                def _schedule():
                    task = asyncio.create_task(self.on_change_callback())
                    self._pending_tasks.add(task)
                    task.add_done_callback(self._pending_tasks.discard)

                self._loop.call_soon_threadsafe(_schedule)

        except Exception as e:
            logger.error("notification_handler_error", error=str(e), payload=payload)
