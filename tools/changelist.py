"""
changelist.py - Fetch all events between two audit points (keyset pagination).
"""
import argparse
import asyncio
import logging
import sys
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Sequence

# Add parent directory to PYTHONPATH so config is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import asyncpg
import structlog
from structlog.stdlib import LoggerFactory

from config import load_config
from db_manager import DatabaseManager

logger = structlog.get_logger(__name__)

# Type alias for the batch handler: receives a list of rows, returns nothing.
BatchHandler = Callable[[Sequence[asyncpg.Record]], Awaitable[None]]


async def _noop_handler(rows: Sequence[asyncpg.Record]) -> None:
    """Default handler — does nothing. Replace with your own implementation."""
    pass


async def run_changelist(
    audit_point_id: int,
    audit_point_end: int,
    batch_size: int,
    row_handler: BatchHandler = _noop_handler,
) -> None:
    """
    Fetch events for audit_point_id received before audit_point_end was created
    (keyset pagination, batch_size per page).

    Args:
        audit_point_id: First audit point ID (events that contain this audit point).
        audit_point_end: Second audit point ID (events received before its creation date).
        batch_size: Number of rows to fetch per page.
        row_handler: Async callable invoked with each batch of rows.
                     Defaults to a no-op. Replace to write, transform, forward, etc.
    """
    config = load_config()
    db = await DatabaseManager.create(
        config.get_db_dsn(),
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
    )

    try:
        async with db.pool.acquire() as conn:
            start_date = await conn.fetchval(
                "SELECT created_at FROM audit_points WHERE id = $1",
                audit_point_id,
            )
            if start_date is None:
                logger.error("audit_point_not_found", id=audit_point_id)
                return

            limit_date = await conn.fetchval(
                "SELECT created_at FROM audit_points WHERE id = $1",
                audit_point_end,
            )
            if limit_date is None:
                logger.error("audit_point_not_found", id=audit_point_end)
                return

            if limit_date <= start_date:
                logger.error(
                    "audit_points_inverted",
                    audit_point_id=audit_point_id,
                    audit_point_end=audit_point_end,
                    start_date=str(start_date),
                    limit_date=str(limit_date),
                    message="audit_point_end must be strictly more recent than audit_point_id",
                )
                return

            logger.info(
                "changelist_start",
                audit_point_id=audit_point_id,
                audit_point_end=audit_point_end,
                limit_date=str(limit_date),
                batch_size=batch_size,
            )

            last_id = 0
            total = 0
            batch_num = 0

            while True:
                batch_num += 1

                rows = await conn.fetch(
                    """
                    SELECT *
                    FROM s3_events
                    WHERE audit_point_ids @> ARRAY[$1::integer]
                      AND received_at < $2
                      AND id > $3
                    ORDER BY id
                    LIMIT $4
                    """,
                    audit_point_id,
                    limit_date,
                    last_id,
                    batch_size,
                )

                if not rows:
                    break

                await row_handler(rows)

                last_id = rows[-1]["id"]
                total += len(rows)

                logger.info(
                    "changelist_batch",
                    batch_num=batch_num,
                    last_id=last_id,
                    total=total,
                )

                if len(rows) < batch_size:
                    break

            logger.info("changelist_done", total_events=total)

    finally:
        await db.pool.close()


def _setup_logging() -> None:
    """Configure stdlib logging and structlog. Must only be called from main()."""
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=logging.INFO,
    )
    structlog.configure(
        processors=[
            structlog.stdlib.filter_by_level,
            structlog.stdlib.add_logger_name,
            structlog.stdlib.add_log_level,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.processors.UnicodeDecoder(),
            structlog.processors.JSONRenderer(),
        ],
        context_class=dict,
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="Fetch events between two audit points (keyset pagination)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python changelist.py --audit-point-id 1 --audit-point-end 2
  python changelist.py --audit-point-id 1 --audit-point-end 2 --batch-size 2048
        """,
    )
    parser.add_argument(
        "--audit-point-id",
        type=int,
        required=True,
        help="First audit point ID (events that contain this audit point)",
    )
    parser.add_argument(
        "--audit-point-end",
        type=int,
        required=True,
        help="Second audit point ID (events received before its creation date)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=1024,
        help="Pagination batch size (default: 1024)",
    )
    args = parser.parse_args()

    if args.batch_size <= 0 or args.batch_size > 100_000:
        logger.error("error", message="--batch-size must be between 1 and 100000")
        sys.exit(1)

    try:
        asyncio.run(
            run_changelist(
                args.audit_point_id,
                args.audit_point_end,
                args.batch_size,
                # row_handler defaults to _noop_handler
            )
        )
    except Exception as e:
        logger.error("changelist_failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
