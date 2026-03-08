"""
DB Inject - Direct insertion of S3 events into the DB for benchmarks (no SQS).
"""
import argparse
import asyncio
import logging
import random
import sys
from datetime import UTC, datetime
from pathlib import Path

# Add parent directory to PYTHONPATH so config is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import structlog
from structlog.stdlib import LoggerFactory
from structlog.types import FilteringBoundLogger

from config import Config, load_config
from db_manager import DatabaseManager
from audit_matcher import AuditPointMatcher
from models import S3Event

def generate_s3_event_dicts(bucket: str, num_events: int) -> list[dict]:
    """
    Generate fake S3 events (same format as sqs_ingest).

    Args:
        bucket: S3 bucket name
        num_events: Number of events to generate

    Returns:
        List of dicts representing S3 events (keys: eventTime, eventName, s3.bucket.name, s3.object.key, etc.)
    """
    prefixes = ['data/', 'logs/', 'uploads/', 'documents/', 'backups/', '']
    event_types = [
        'ObjectCreated:Put',
        'ObjectCreated:Post',
        'ObjectCreated:Copy',
        'ObjectRemoved:Delete',
    ]

    events = []
    for i in range(num_events):
        prefix = random.choice(prefixes)
        filename = f"file_{i}_{random.randint(1000, 9999)}.txt"
        events.append({
            'eventTime': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
            'eventName': random.choice(event_types),
            's3': {
                'bucket': {'name': bucket},
                'object': {
                    'key': f'{prefix}{filename}',
                    'size': random.randint(100, 10_000_000),
                    'versionId': f'v{random.randint(1, 100)}' if random.random() > 0.3 else None,
                },
            },
        })
    return events


def dicts_to_s3_events(
    event_dicts: list[dict],
    matcher: AuditPointMatcher,
) -> list[S3Event]:
    """Convert S3 event dicts to S3Event models with audit_point_ids."""
    out: list[S3Event] = []
    for d in event_dicts:
        s3 = d.get('s3', {})
        bucket = s3.get('bucket', {}).get('name', '')
        obj = s3.get('object', {})
        key = obj.get('key', '')
        size = int(obj.get('size', 0))
        version_id: str | None = obj.get('versionId')
        event_name = d.get('eventName', 'ObjectCreated:Put')
        event_time_str = d.get('eventTime', '')
        try:
            event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
        except Exception:
            event_time = datetime.now(UTC)

        audit_point_ids = matcher.get_matching_audit_points(bucket, key)
        out.append(S3Event(
            event_time=event_time,
            event_name=event_name,
            bucket=bucket,
            object_key=key,
            size=size,
            version_id=version_id,
            audit_point_ids=audit_point_ids,
        ))
    return out


async def run_inject(config: Config, bucket: str, num_events: int, batch_size: int, logger: FilteringBoundLogger | None = None) -> None:
    """Create pool and matcher, generate events and insert them into the DB."""
    log = logger or structlog.get_logger(__name__)
    db = await DatabaseManager.create(
        config.get_db_dsn(),
        min_size=config.db_pool_min_size,
        max_size=config.db_pool_max_size,
    )

    try:
        audit_points = await db.load_audit_points()
        matcher = AuditPointMatcher(audit_points)
        log.info("audit_points_loaded", count=len(audit_points))

        log.info("generating_events", bucket=bucket, num_events=num_events)
        event_dicts = generate_s3_event_dicts(bucket, num_events)
        s3_events = dicts_to_s3_events(event_dicts, matcher)

        total_inserted: int = 0
        total_time: float = 0.0
        batches: int = 0

        log.info("db_inject_insert_start", total_events=len(s3_events), batch_size=batch_size)
        for i in range(0, len(s3_events), batch_size):
            batch = s3_events[i : i + batch_size]
            n, elapsed = await db.insert_events_batch(batch)
            total_inserted += n
            total_time += elapsed
            batches += 1

        log.info(
            "db_inject_complete",
            bucket=bucket,
            events_requested=num_events,
            events_inserted=total_inserted,
            batches=batches,
            batch_size=batch_size,
            total_db_seconds=round(total_time, 2),
            events_per_second=round(total_inserted / total_time, 2) if total_time > 0 else 0,
        )
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
    log = structlog.get_logger(__name__)

    parser = argparse.ArgumentParser(
        description="Inject S3 events directly into the DB for benchmarks (no SQS)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python db_inject.py --bucket my-bucket --events 10000
  python db_inject.py --bucket my-bucket --events 50000 --batch-size 500
        """,
    )
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name for generated events")
    parser.add_argument("--events", type=int, required=True, help="Number of events to insert")
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        help="Insert batch size (default: 500)",
    )
    args = parser.parse_args()

    if args.events <= 0:
        log.error("error", message="--events must be > 0")
        sys.exit(1)
    if args.batch_size <= 0 or args.batch_size > 10_000:
        log.error("error", message="--batch-size must be between 1 and 10000")
        sys.exit(1)

    try:
        asyncio.run(
            run_inject(
                load_config(),
                args.bucket,
                args.events,
                args.batch_size,
                logger=log,
            )
        )
    except Exception as e:
        log.error("db_inject_failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
