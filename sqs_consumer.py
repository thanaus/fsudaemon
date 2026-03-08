"""
SQS Consumer - Asynchronous consumption of SQS messages
"""
import time
from typing import List, Dict, Any, Optional
import aioboto3
from botocore.exceptions import ClientError
import structlog

logger = structlog.get_logger(__name__)


class SQSConsumer:
    """Asynchronous consumer for Amazon SQS"""

    def __init__(
        self,
        queue_url: str,
        instruments: dict,
        region_name: str = "us-east-1",
        aws_access_key_id: Optional[str] = None,
        aws_secret_access_key: Optional[str] = None,
        max_messages: int = 10,
        wait_time_seconds: int = 20,
        visibility_timeout: int = 30,
        endpoint_url: Optional[str] = None,
    ):
        """
        Initializes the SQS consumer.

        Args:
            queue_url: SQS queue URL
            instruments: OpenTelemetry instruments dict
            region_name: AWS region
            aws_access_key_id: AWS access key (optional if IAM role)
            aws_secret_access_key: AWS secret key (optional if IAM role)
            max_messages: Max messages per batch (1-10)
            wait_time_seconds: Long polling duration (0-20)
            visibility_timeout: Message visibility timeout
            endpoint_url: Optional SQS endpoint (e.g. http://localhost:4566 for LocalStack)
        """
        self.queue_url = queue_url
        self.instruments = instruments
        self.region_name = region_name
        self.aws_access_key_id = aws_access_key_id
        self.aws_secret_access_key = aws_secret_access_key
        self.max_messages = max_messages
        self.wait_time_seconds = wait_time_seconds
        self.visibility_timeout = visibility_timeout
        self.endpoint_url = endpoint_url

        self.session = aioboto3.Session(
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=region_name
        )

        self._client = None
        self._client_kw: Dict[str, Any] = {}
        if endpoint_url:
            self._client_kw["endpoint_url"] = endpoint_url

        logger.debug(
            "sqs_consumer_initialized",
            queue_url=queue_url,
            region=region_name,
            max_messages=max_messages
        )

    async def start(self) -> None:
        """
        Opens the persistent SQS client. Must be called before receiving messages.
        """
        self._client = await self.session.client("sqs", **self._client_kw).__aenter__()
        logger.debug("sqs_client_started")

    async def stop(self) -> None:
        """
        Closes the persistent SQS client. Must be called on shutdown.
        """
        if self._client is not None:
            try:
                await self._client.__aexit__(None, None, None)
            except Exception as e:
                logger.warning("sqs_client_close_error", error=str(e))
            finally:
                self._client = None
                logger.debug("sqs_client_stopped")

    async def receive_message(self) -> List[Dict[str, Any]]:
        """
        Receives messages from SQS queue with long polling.

        Returns:
            List of SQS messages (can be empty if no messages)
        """
        try:
            start = time.perf_counter()
            response = await self._client.receive_message(
                QueueUrl=self.queue_url,
                MaxNumberOfMessages=self.max_messages,
                WaitTimeSeconds=self.wait_time_seconds,
                VisibilityTimeout=self.visibility_timeout,
                AttributeNames=['All'],
                MessageAttributeNames=['All']
            )
            self.instruments["sqs_receive_message_seconds"].record(time.perf_counter() - start)

            messages = response.get('Messages', [])

            if messages:
                logger.debug("messages_received", count=len(messages))

            return messages

        except ClientError as e:
            logger.error(
                "sqs_receive_error",
                error_code=e.response['Error']['Code'],
                error_message=e.response['Error']['Message']
            )
            raise
        except Exception as e:
            logger.error("sqs_receive_unexpected_error", error=str(e))
            raise

    async def delete_message_batch(self, receipt_handles: List[str]) -> int:
        """
        Deletes multiple messages in batch (up to 10).

        Args:
            receipt_handles: List of receipt handles to delete

        Returns:
            Number of successfully deleted messages
        """
        if not receipt_handles:
            return 0

        batch_size = 10
        deleted_count = 0

        try:
            for i in range(0, len(receipt_handles), batch_size):
                batch = receipt_handles[i:i + batch_size]

                entries = [
                    {
                        'Id': str(idx),
                        'ReceiptHandle': handle
                    }
                    for idx, handle in enumerate(batch)
                ]

                start = time.perf_counter()
                response = await self._client.delete_message_batch(
                    QueueUrl=self.queue_url,
                    Entries=entries
                )
                self.instruments["sqs_delete_message_batch_seconds"].record(time.perf_counter() - start)

                deleted_count += len(response.get('Successful', []))

                if 'Failed' in response and response['Failed']:
                    logger.warning(
                        "batch_delete_partial_failure",
                        failed_count=len(response['Failed'])
                    )

            logger.debug("messages_deleted_batch", count=deleted_count)
            return deleted_count

        except Exception as e:
            logger.error("sqs_batch_delete_error", error=str(e))
            return deleted_count
