"""
SQS Ingest - Script to inject test S3 events into SQS queue
"""
import argparse
import json
import sys
import random
from pathlib import Path
from datetime import datetime, timezone

# Add parent directory to PYTHONPATH so config is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import logging
import boto3
from botocore.exceptions import ClientError
from config import load_config
import structlog
from structlog.stdlib import LoggerFactory

# Configure standard Python logging (same as main.py)
logging.basicConfig(
    format="%(message)s",
    stream=sys.stdout,
    level=logging.INFO,
)

# Structlog configuration (same as main.py)
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

logger = structlog.get_logger(__name__)


def generate_s3_events(bucket: str, num_events: int) -> list:
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
        'ObjectRemoved:Delete'
    ]
    
    events = []
    for i in range(num_events):
        prefix = random.choice(prefixes)
        filename = f"file_{i}_{random.randint(1000, 9999)}.txt"
        
        event = {
            'eventVersion': '2.1',
            'eventSource': 'aws:s3',
            'awsRegion': 'us-east-1',
            'eventTime': datetime.now(timezone.utc).isoformat().replace('+00:00', 'Z'),
            'eventName': random.choice(event_types),
            's3': {
                'bucket': {
                    'name': bucket,
                    'arn': f'arn:aws:s3:::{bucket}'
                },
                'object': {
                    'key': f'{prefix}{filename}',
                    'size': random.randint(100, 10000000),
                    'versionId': f'v{random.randint(1, 100)}'
                }
            }
        }
        events.append(event)
    
    return events


def send_to_sqs(queue_url: str, messages: list, sqs_client) -> int:
    """
    Send messages to SQS using batch API.
    
    Args:
        queue_url: SQS queue URL
        messages: List of message bodies (JSON strings)
        sqs_client: boto3 SQS client
    
    Returns:
        Number of messages successfully sent
    """
    sent_count = 0
    failed_count = 0
    
    # SQS SendMessageBatch accepts max 10 messages at a time
    batch_size = 10
    
    for i in range(0, len(messages), batch_size):
        batch = messages[i:i + batch_size]
        
        # Prepare entries for batch
        entries = [
            {
                'Id': str(idx),
                'MessageBody': msg
            }
            for idx, msg in enumerate(batch)
        ]
        
        try:
            response = sqs_client.send_message_batch(
                QueueUrl=queue_url,
                Entries=entries
            )
            
            successful = len(response.get('Successful', []))
            failed = len(response.get('Failed', []))
            
            sent_count += successful
            failed_count += failed
            
            if failed > 0:
                logger.warning(
                    "batch_send_partial_failure",
                    successful=successful,
                    failed=failed,
                    failures=response.get('Failed', [])
                )
        
        except ClientError as e:
            logger.error(
                "batch_send_error",
                error=str(e),
                batch_size=len(batch)
            )
            failed_count += len(batch)
    
    return sent_count, failed_count


def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Inject test S3 events into SQS queue',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Send 1000 messages (10,000 events) for bucket 'my-bucket'
  python sqs_ingest.py --bucket my-bucket --messages 1000
  
  # Send 10,000 messages (100,000 events) with custom events per message
  python sqs_ingest.py --bucket test-bucket --messages 10000 --events-per-message 10
        """
    )
    
    parser.add_argument(
        '--bucket',
        type=str,
        required=True,
        help='S3 bucket name to use for generated events'
    )
    
    parser.add_argument(
        '--messages',
        type=int,
        required=True,
        help='Number of SQS messages to send'
    )
    
    parser.add_argument(
        '--events-per-message',
        type=int,
        default=10,
        help='Number of S3 events per SQS message (default: 10, realistic for AWS)'
    )
    
    args = parser.parse_args()
    
    # Validate arguments
    if args.messages <= 0:
        logger.error("error", message="--messages must be positive")
        sys.exit(1)
    
    if args.events_per_message <= 0 or args.events_per_message > 100:
        logger.error("error", message="--events-per-message must be between 1 and 100")
        sys.exit(1)
    
    total_events = args.messages * args.events_per_message
    
    logger.info(
        "sqs_ingest_started",
        bucket=args.bucket,
        messages_to_send=args.messages,
        events_per_message=args.events_per_message,
        total_events=total_events
    )
    
    # Load configuration
    try:
        config = load_config()
    except Exception as e:
        logger.error("config_load_failed", error=str(e))
        sys.exit(1)
    
    # Create SQS client
    try:
        client_kw = {
            "region_name": config.aws_region,
            "aws_access_key_id": config.aws_access_key_id,
            "aws_secret_access_key": config.aws_secret_access_key,
        }
        if config.aws_endpoint_url:
            client_kw["endpoint_url"] = config.aws_endpoint_url
        sqs = boto3.client("sqs", **client_kw)
        logger.info("sqs_client_created", queue_url=config.sqs_queue_url)
    except Exception as e:
        logger.error("sqs_client_creation_failed", error=str(e))
        sys.exit(1)
    
    # Generate all S3 events
    logger.info("generating_events", total=total_events)
    all_events = generate_s3_events(args.bucket, total_events)
    logger.info("events_generated", count=len(all_events))
    
    # Group events into SQS messages
    logger.info("grouping_into_messages", messages=args.messages)
    messages = []
    
    for i in range(args.messages):
        start_idx = i * args.events_per_message
        end_idx = start_idx + args.events_per_message
        records = all_events[start_idx:end_idx]
        
        message_body = json.dumps({'Records': records})
        messages.append(message_body)
    
    logger.info("messages_prepared", count=len(messages))
    
    # Send to SQS
    logger.info("sending_to_sqs", queue_url=config.sqs_queue_url)
    sent_count, failed_count = send_to_sqs(config.sqs_queue_url, messages, sqs)
    
    # Summary
    logger.info(
        "sqs_ingest_complete",
        messages_sent=sent_count,
        messages_failed=failed_count,
        total_events_sent=sent_count * args.events_per_message,
        success_rate=round(sent_count / args.messages * 100, 2) if args.messages > 0 else 0
    )
    
    if failed_count > 0:
        logger.warning("some_messages_failed", failed=failed_count)
        sys.exit(1)
    
    logger.info("all_messages_sent_successfully")


if __name__ == "__main__":
    main()
