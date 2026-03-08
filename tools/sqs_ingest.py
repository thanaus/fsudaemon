"""
SQS Ingest - Script to inject test S3 events into SQS queue
"""
import argparse
import json
import logging
import random
import sys
from datetime import UTC, datetime
from pathlib import Path

# Add parent directory to PYTHONPATH so config is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import boto3
import structlog
from botocore.exceptions import ClientError
from structlog.stdlib import LoggerFactory
from structlog.types import FilteringBoundLogger

from config import Config, load_config

def generate_s3_events(bucket: str, num_events: int) -> list[dict]:
    """
    Generate fake S3 events for the specified bucket.

    Args:
        bucket: S3 bucket name
        num_events: Number of events to generate

    Returns:
        List of S3 event dictionaries
    """
    prefixes = ['data/', 'logs/', 'uploads/', 'documents/', 'backups/', '']
    event_types = [
        'ObjectCreated:Put',
        'ObjectCreated:Post',
        'ObjectCreated:Copy',
        'ObjectRemoved:Delete',
    ]

    events: list[dict] = []
    for i in range(num_events):
        prefix = random.choice(prefixes)
        filename = f"file_{i}_{random.randint(1000, 9999)}.txt"
        events.append({
            'eventVersion': '2.1',
            'eventSource': 'aws:s3',
            'awsRegion': 'us-east-1',
            'eventTime': datetime.now(UTC).isoformat().replace('+00:00', 'Z'),
            'eventName': random.choice(event_types),
            's3': {
                'bucket': {
                    'name': bucket,
                    'arn': f'arn:aws:s3:::{bucket}',
                },
                'object': {
                    'key': f'{prefix}{filename}',
                    'size': random.randint(100, 10_000_000),
                    'versionId': f'v{random.randint(1, 100)}' if random.random() > 0.3 else None,
                },
            },
        })
    return events


def send_to_sqs(queue_url: str, messages: list[str], sqs_client, logger: FilteringBoundLogger | None = None) -> tuple[int, int]:
    """
    Send messages to SQS using batch API.

    Args:
        queue_url: SQS queue URL
        messages: List of message bodies (JSON strings)
        sqs_client: boto3 SQS client
        logger: Injected structlog logger. Defaults to a module-level logger if None.

    Returns:
        Tuple of (sent_count, failed_count)
    """
    log = logger or structlog.get_logger(__name__)
    sent_count: int = 0
    failed_count: int = 0

    # SQS SendMessageBatch accepts max 10 messages at a time
    batch_size = 10

    for i in range(0, len(messages), batch_size):
        batch = messages[i : i + batch_size]
        entries = [
            {'Id': str(idx), 'MessageBody': msg}
            for idx, msg in enumerate(batch)
        ]

        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries,
            )
            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))
            sent_count += successful
            failed_count += failed

            if failed > 0:
                log.warning(
                    "batch_send_partial_failure",
                    successful=successful,
                    failed=failed,
                    failures=response.get('Failed', []),
                )

        except ClientError as e:
            log.error(
                "batch_send_error",
                error=str(e),
                batch_size=len(batch),
            )
            failed_count += len(batch)

    return sent_count, failed_count


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


def run_ingest(config: Config, bucket: str, messages: int, events_per_message: int, logger: FilteringBoundLogger | None = None) -> None:
    """Create SQS client, generate events and send them to the queue.

    Args:
        config: Application configuration (injected, not loaded internally).
        bucket: S3 bucket name for generated events.
        messages: Number of SQS messages to send.
        events_per_message: Number of S3 events per SQS message.
        logger: Injected structlog logger. Defaults to a module-level logger if None.
    """
    log = logger or structlog.get_logger(__name__)

    total_events = messages * events_per_message
    log.info(
        "sqs_ingest_started",
        bucket=bucket,
        messages_to_send=messages,
        events_per_message=events_per_message,
        total_events=total_events,
    )

    client_kw: dict = {"region_name": config.aws_region}
    if config.aws_access_key_id:
        client_kw["aws_access_key_id"] = config.aws_access_key_id.get_secret_value()
    if config.aws_secret_access_key:
        client_kw["aws_secret_access_key"] = config.aws_secret_access_key.get_secret_value()
    if config.aws_endpoint_url:
        client_kw["endpoint_url"] = config.aws_endpoint_url

    sqs = boto3.client("sqs", **client_kw)
    log.info("sqs_client_created", queue_url=config.sqs_queue_url)

    log.info("generating_events", total=total_events)
    all_events = generate_s3_events(bucket, total_events)
    log.info("events_generated", count=len(all_events))

    msg_list: list[str] = [
        json.dumps({'Records': all_events[i * events_per_message : (i + 1) * events_per_message]})
        for i in range(messages)
    ]
    log.info("messages_prepared", count=len(msg_list))

    log.info("sending_to_sqs", queue_url=config.sqs_queue_url)
    sent_count, failed_count = send_to_sqs(config.sqs_queue_url, msg_list, sqs, logger=log)

    log.info(
        "sqs_ingest_complete",
        messages_sent=sent_count,
        messages_failed=failed_count,
        total_events_sent=sent_count * events_per_message,
        success_rate=round(sent_count / messages * 100, 2) if messages > 0 else 0,
    )

    if failed_count > 0:
        log.warning("some_messages_failed", failed=failed_count)
        sys.exit(1)

    log.info("all_messages_sent_successfully")


def main() -> None:
    _setup_logging()
    log = structlog.get_logger(__name__)

    parser = argparse.ArgumentParser(
        description="Inject test S3 events into SQS queue",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  python sqs_ingest.py --bucket my-bucket --messages 1000
  python sqs_ingest.py --bucket test-bucket --messages 10000 --events-per-message 10
        """,
    )
    parser.add_argument("--bucket", type=str, required=True, help="S3 bucket name to use for generated events")
    parser.add_argument("--messages", type=int, required=True, help="Number of SQS messages to send")
    parser.add_argument(
        "--events-per-message",
        type=int,
        default=10,
        help="Number of S3 events per SQS message (default: 10, realistic for AWS)",
    )
    args = parser.parse_args()

    if args.messages <= 0:
        log.error("error", message="--messages must be > 0")
        sys.exit(1)
    if args.events_per_message <= 0 or args.events_per_message > 100:
        log.error("error", message="--events-per-message must be between 1 and 100")
        sys.exit(1)

    try:
        run_ingest(
            load_config(),
            args.bucket,
            args.messages,
            args.events_per_message,
            logger=log,
        )
    except Exception as e:
        log.error("sqs_ingest_failed", error=str(e))
        sys.exit(1)


if __name__ == "__main__":
    main()
