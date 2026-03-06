"""
Event Processor - Orchestration of S3 event processing
"""
import json
from typing import List, Dict, Any
from datetime import datetime, timezone
from models import S3Event
from audit_matcher import AuditPointMatcher
from db_manager import DatabaseManager
from telemetry import instruments
import structlog

logger = structlog.get_logger(__name__)


class EventProcessor:
    """S3 event processor from SQS to PostgreSQL"""
    
    def __init__(self, matcher: AuditPointMatcher, db_manager: DatabaseManager):
        """
        Initializes the processor.
        
        Args:
            matcher: Audit point matcher
            db_manager: Database manager
        """
        self.matcher = matcher
        self.db_manager = db_manager

    async def process_message(self, message: Dict[str, Any]) -> int:
        """
        Processes an SQS message containing S3 events.

        Args:
            message: Raw SQS message

        Returns:
            Number of errors encountered for this message (0 = success)
        """
        _otel = instruments()
        _otel["messages_received"].add(1)
        errors = 0

        try:
            body = json.loads(message['Body'])
            records = body.get('Records', [])

            if not records:
                logger.warning("no_records_in_message", message_id=message.get('MessageId'))
                return 0

            events_to_insert = []
            events_kept = 0
            events_discarded = 0
            total_associations = 0

            for record in records:
                try:
                    event = self._parse_s3_record(record)

                    if event:
                        events_to_insert.append(event)
                        events_kept += 1
                        total_associations += len(event.audit_point_ids)
                        _otel["events_kept"].add(1)
                        _otel["total_associations"].add(len(event.audit_point_ids))
                    else:
                        events_discarded += 1
                        _otel["events_discarded"].add(1)

                except Exception as e:
                    logger.error("record_parse_error", error=str(e), record=record)
                    errors += 1
                    _otel["errors"].add(1)

            if events_to_insert:
                _, db_time = await self.db_manager.insert_events_batch(events_to_insert)
                _otel["messages_processed"].add(1)
                _otel["db_insert_seconds"].record(db_time)

                logger.debug(
                    "message_processed",
                    events_kept=events_kept,
                    events_discarded=events_discarded,
                    associations=total_associations
                )
            else:
                logger.debug(
                    "message_no_relevant_events",
                    events_discarded=events_discarded,
                    reason="no_audit_points_matched"
                )

            return errors

        except json.JSONDecodeError as e:
            logger.error("message_json_parse_error", error=str(e))
            errors += 1
            _otel["errors"].add(1)
            return errors
        except Exception as e:
            logger.error("message_processing_error", error=str(e))
            errors += 1
            _otel["errors"].add(1)
            return errors
    
    def _parse_s3_record(self, record: Dict[str, Any]) -> S3Event | None:
        """
        Parses an S3 record and checks if it matches audit points.
        
        Args:
            record: S3 record from SQS message
        
        Returns:
            S3Event if event matches audit points, None otherwise
        """
        try:
            # Extract event information
            event_name = record.get('eventName', '')
            event_time_str = record.get('eventTime', '')
            
            s3_data = record.get('s3', {})
            bucket = s3_data.get('bucket', {}).get('name', '')
            obj = s3_data.get('object', {})
            key = obj.get('key', '')
            size = obj.get('size', 0)
            version_id = obj.get('versionId')
            
            # Parse date
            try:
                event_time = datetime.fromisoformat(event_time_str.replace('Z', '+00:00'))
            except Exception:
                event_time = datetime.now(timezone.utc)
            
            # Match against audit points
            matching_audit_point_ids = self.matcher.get_matching_audit_points(bucket, key)
            
            if not matching_audit_point_ids:
                logger.debug(
                    "event_no_match",
                    bucket=bucket,
                    key=key,
                    event_name=event_name
                )
                return None
            
            # Create event
            event = S3Event(
                event_time=event_time,
                event_name=event_name,
                bucket=bucket,
                object_key=key,
                size=size,
                version_id=version_id,
                audit_point_ids=matching_audit_point_ids
            )
            
            logger.debug(
                "event_matched",
                bucket=bucket,
                key=key,
                event_name=event_name,
                audit_points=len(matching_audit_point_ids)
            )
            
            return event
            
        except Exception as e:
            logger.error("s3_record_parse_error", error=str(e), record=record)
            raise
