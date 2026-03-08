"""
Main - Entry point for S3 Event Processor
"""
import asyncio
import argparse
import logging
import signal
import sys
import structlog
from structlog.stdlib import LoggerFactory

from config import load_config
from db_manager import DatabaseManager, AuditPointsListener
from audit_matcher import AuditPointMatcher
from sqs_consumer import SQSConsumer
from event_processor import EventProcessor
from telemetry import init_metrics, instruments, shutdown_metrics

# Minimal bootstrap logger — used only if configuration fails before structlog
# is fully configured. Replaced by the structured logger once setup_logging() runs.
logging.basicConfig(format="%(message)s", stream=sys.stdout, level=logging.INFO)
logger = structlog.get_logger(__name__)

# Global flag for graceful shutdown
shutdown_event = asyncio.Event()


def setup_logging(log_level_str: str) -> None:
    """
    Configures stdlib logging and structlog from the application log level.
    Must be called after load_config() so the level is known.

    Args:
        log_level_str: Log level string from config (e.g. "DEBUG", "INFO", "WARNING")
    """
    log_level = getattr(logging, log_level_str, logging.INFO)

    # Reconfigure stdlib root logger with the resolved level
    logging.basicConfig(
        format="%(message)s",
        stream=sys.stdout,
        level=log_level,
        force=True,  # override the bootstrap basicConfig called at module level
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
        # make_filtering_bound_logger applies the level filter directly on the
        # structlog wrapper — filter_by_level alone is not sufficient.
        wrapper_class=structlog.make_filtering_bound_logger(log_level),
        context_class=dict,
        logger_factory=LoggerFactory(),
        cache_logger_on_first_use=True,
    )


def signal_handler(signum, frame):
    """Handler for shutdown signals (SIGINT, SIGTERM)"""
    logger.debug("shutdown_signal_received", signal=signum)
    shutdown_event.set()


async def process_messages_loop(
    sqs_consumer: SQSConsumer,
    processor: EventProcessor
) -> None:
    """
    Main loop for processing SQS messages.

    Args:
        sqs_consumer: SQS consumer
        processor: Event processor
    """
    logger.debug("message_processing_loop_started")

    consecutive_errors = 0
    max_consecutive_errors = 5

    while not shutdown_event.is_set():
        try:
            messages = await sqs_consumer.receive_message()

            if not messages:
                consecutive_errors = 0
                continue

            logger.debug("messages_received", count=len(messages))

            receipt_handles_to_delete = []

            for message in messages:
                try:
                    errors = await processor.process_message(message)
                    if errors == 0:
                        receipt_handles_to_delete.append(message['ReceiptHandle'])
                    else:
                        logger.warning(
                            "message_not_deleted_due_to_errors",
                            message_id=message.get('MessageId'),
                            errors=errors
                        )

                except Exception as e:
                    logger.error(
                        "message_processing_failed",
                        error=str(e),
                        message_id=message.get('MessageId')
                    )

            if receipt_handles_to_delete:
                deleted = await sqs_consumer.delete_message_batch(receipt_handles_to_delete)
                logger.debug("messages_deleted_from_sqs", count=deleted)

            consecutive_errors = 0

        except KeyboardInterrupt:
            logger.debug("keyboard_interrupt_received")
            shutdown_event.set()
            break
        except Exception as e:
            consecutive_errors += 1
            logger.error(
                "processing_loop_error",
                error=str(e),
                consecutive_errors=consecutive_errors
            )

            if consecutive_errors >= max_consecutive_errors:
                logger.error("max_consecutive_errors_reached", max=max_consecutive_errors)
                shutdown_event.set()
                break

            # Exponential backoff
            await asyncio.sleep(min(2 ** consecutive_errors, 60))

    logger.debug("message_processing_loop_stopped")


async def sync_audit_points(
    matcher: AuditPointMatcher,
    db_manager: DatabaseManager
) -> None:
    """
    Synchronizes audit points from database to matcher.
    Used at startup and when NOTIFY triggers a change.

    Args:
        matcher: Audit point matcher
        db_manager: Database manager
    """
    try:
        logger.debug("syncing_audit_points")
        audit_points = await db_manager.load_audit_points()
        matcher.load_audit_points(audit_points)
        logger.debug("audit_points_synced", count=len(audit_points))
    except ValueError as e:
        # load_audit_points() raises ValueError if an AuditPoint has an empty bucket
        # or a None prefix. The matcher is NOT updated — it keeps its previous state.
        # This is logged as a critical data quality issue: the offending row must be
        # fixed in the database before the next NOTIFY triggers a reload.
        logger.error(
            "audit_sync_invalid_data",
            error=str(e),
            matcher_state="unchanged",
            action_required="fix invalid audit_point row in database",
        )
    except Exception as e:
        logger.error("audit_sync_error", error=str(e), matcher_state="unchanged")


async def listen_audit_points_changes(
    matcher: AuditPointMatcher,
    db_manager: DatabaseManager
) -> None:
    """
    Listens to PostgreSQL LISTEN/NOTIFY notifications for audit point changes.
    Automatically reloads audit points in real-time.

    Args:
        matcher: Audit point matcher
        db_manager: Database manager
    """
    logger.debug("audit_listen_loop_starting")

    async def on_change():
        await sync_audit_points(matcher, db_manager)

    listener = AuditPointsListener(db_manager, on_change)

    try:
        await listener.start_listening()

        while not shutdown_event.is_set():
            await asyncio.sleep(1)

    except Exception as e:
        logger.error("audit_listen_loop_error", error=str(e))
    finally:
        await listener.stop_listening()
        logger.debug("audit_listen_loop_stopped")


def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='S3 Event Processor')
    return parser.parse_args()


async def main() -> None:
    """Main entry point"""
    parse_args()

    logger.info("s3_event_processor_starting")

    # 1. Load configuration
    try:
        config = load_config()
    except Exception as e:
        logger.error("configuration_load_failed", error=str(e))
        sys.exit(1)

    # 2. Apply log level from config — must happen before any other log call
    # so that DEBUG messages are visible when LOG_LEVEL=DEBUG, and filtered
    # when LOG_LEVEL=INFO or higher.
    setup_logging(config.log_level)
    logger.info("configuration_loaded", log_level=config.log_level)

    # 3. OpenTelemetry - metrics exported to stdout, optionally to OTLP collector
    init_metrics(
        export_interval_seconds=config.otel_export_interval_seconds,
        otlp_endpoint=config.otlp_endpoint,
    )
    instr = instruments()
    logger.info("telemetry_initialized",
                export="stdout",
                otlp=bool(config.otlp_endpoint),
                export_interval_seconds=config.otel_export_interval_seconds)

    # 4. Create PostgreSQL connection pool
    try:
        db_manager = await DatabaseManager.create(
            config.get_db_dsn(),
            min_size=config.db_pool_min_size,
            max_size=config.db_pool_max_size
        )

        if not await db_manager.health_check():
            logger.error("database_health_check_failed")
            sys.exit(1)

        logger.info("database_connection_ok",
                    pool_min=config.db_pool_min_size,
                    pool_max=config.db_pool_max_size)
    except Exception as e:
        logger.error("database_connection_failed", error=str(e))
        sys.exit(1)

    # 5. Load audit points
    try:
        audit_points = await db_manager.load_audit_points()
        matcher = AuditPointMatcher(audit_points)

        stats = matcher.get_stats()
        logger.info("audit_matcher_initialized", stats=stats)

        if len(audit_points) == 0:
            logger.warning(
                "no_audit_points_configured",
                message="No audit points found in database. All SQS messages will be discarded."
            )
    except Exception as e:
        logger.error("audit_points_load_failed", error=str(e))
        await db_manager.pool.close()
        sys.exit(1)

    # 6. Create and start SQS consumer (persistent client)
    try:
        sqs_consumer = SQSConsumer(
            queue_url=config.sqs_queue_url,
            instruments=instr,
            region_name=config.aws_region,
            # Both aws_access_key_id and aws_secret_access_key are SecretStr in Config.
            # .get_secret_value() is required to extract the plain string; passing the
            # SecretStr object directly would cause a type mismatch and would expose the
            # repr ("**********") rather than the actual value to boto3.
            aws_access_key_id=config.aws_access_key_id.get_secret_value() if config.aws_access_key_id else None,
            aws_secret_access_key=config.aws_secret_access_key.get_secret_value() if config.aws_secret_access_key else None,
            max_messages=config.sqs_batch_size,
            wait_time_seconds=config.sqs_wait_time_seconds,
            visibility_timeout=config.sqs_visibility_timeout,
            endpoint_url=config.aws_endpoint_url,
        )
        await sqs_consumer.start()
        logger.info("sqs_consumer_initialized",
                    queue_url=config.sqs_queue_url,
                    region=config.aws_region,
                    batch_size=config.sqs_batch_size)
    except Exception as e:
        logger.error("sqs_consumer_init_failed", error=str(e))
        await db_manager.pool.close()
        sys.exit(1)

    # 7. Create event processor
    processor = EventProcessor(matcher, db_manager, instruments=instr)
    logger.debug("event_processor_initialized")

    # 8. Configure signal handlers
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    # 9. Start workers
    tasks = [
        asyncio.create_task(process_messages_loop(sqs_consumer, processor)),
        asyncio.create_task(listen_audit_points_changes(matcher, db_manager)),
    ]

    logger.info(
        "workers_started",
        workers=["sqs_message_processor", "audit_points_listener"],
        metrics_export_interval_seconds=config.otel_export_interval_seconds,
    )

    # 10. Wait for shutdown
    await shutdown_event.wait()

    # 11. Shutdown gracefully
    logger.debug("shutting_down")

    for task in tasks:
        task.cancel()

    await asyncio.gather(*tasks, return_exceptions=True)

    await sqs_consumer.stop()

    shutdown_metrics()

    await db_manager.pool.close()

    logger.info("shutdown_complete")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.debug("keyboard_interrupt_main")
    except Exception as e:
        logger.error("main_unexpected_error", error=str(e))
        sys.exit(1)
