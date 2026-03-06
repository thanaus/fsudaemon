"""
OpenTelemetry metrics - export stats to stdout (single structlog line).
"""
from typing import Any, Dict, Optional

import structlog
from opentelemetry import metrics
from opentelemetry.sdk.metrics import MeterProvider
from opentelemetry.sdk.metrics.export import (
    MetricExportResult,
    MetricExporter,
    PeriodicExportingMetricReader,
)

logger = structlog.get_logger(__name__)

# OTel metric name -> structlog key (same as legacy processing_stats)
_METRIC_NAME_TO_KEY = {
    "fsudaemon.messages_received": "messages_received",
    "fsudaemon.messages_processed": "messages_processed",
    "fsudaemon.events_kept": "events_kept",
    "fsudaemon.events_discarded": "events_discarded",
    "fsudaemon.total_associations": "total_associations",
    "fsudaemon.errors": "errors",
    "fsudaemon.db_insert_seconds": "db_time_seconds",
    "fsudaemon.sqs_receive_message_seconds": "sqs_receive_message_seconds",
    "fsudaemon.sqs_delete_message_batch_seconds": "sqs_delete_message_batch_seconds",
}


class StructlogMetricExporter(MetricExporter):
    """
    Exporter that sends metrics as a single structlog line (event="processing_stats", ...).
    Logs the delta since last export (activity over the last period, e.g. last minute).
    """

    def __init__(self) -> None:
        super().__init__()
        self._last: Dict[str, Any] = {}

    def export(self, metrics_data, timeout_millis: float = 10_000, **kwargs) -> MetricExportResult:
        if metrics_data is None:
            return MetricExportResult.SUCCESS
        try:
            current = _extract_metrics_payload(metrics_data)
            if not current:
                return MetricExportResult.SUCCESS
            # Delta = activity over last period (not cumulative)
            delta: Dict[str, Any] = {}
            for key, value in current.items():
                prev = self._last.get(key, 0)
                if isinstance(value, (int, float)):
                    d = value - (prev if isinstance(prev, (int, float)) else 0)
                    if isinstance(value, int):
                        delta[key] = max(0, int(d))
                    else:
                        delta[key] = round(max(0.0, float(d)), 2)
                else:
                    delta[key] = value
            self._last = dict(current)
            if delta:
                logger.info("processing_stats", **delta)
        except Exception as e:
            logger.warning("metrics_export_error", error=str(e))
        return MetricExportResult.SUCCESS

    def force_flush(self, timeout_millis: float = 10_000) -> bool:
        return True

    def shutdown(self, timeout_millis: float = 30_000, **kwargs) -> None:
        pass


def _extract_metrics_payload(metrics_data) -> Dict[str, Any]:
    """Extract a dict {messages_received: int, ..., db_time_seconds: float} from MetricsData."""
    out: Dict[str, Any] = {}
    for resource_metrics in getattr(metrics_data, "resource_metrics", []) or []:
        for scope_metrics in getattr(resource_metrics, "scope_metrics", []) or []:
            for metric in getattr(scope_metrics, "metrics", []) or []:
                name = getattr(metric, "name", None)
                if not name or name not in _METRIC_NAME_TO_KEY:
                    continue
                key = _METRIC_NAME_TO_KEY[name]
                data = getattr(metric, "data", None)
                if data is None:
                    continue
                # Sum (counter) -> NumberDataPoint.value
                data_points = getattr(data, "data_points", None) or []
                if not data_points:
                    continue
                if hasattr(data_points[0], "value"):
                    # Counter: sum of values (usually a single point)
                    total = sum(getattr(dp, "value", 0) or 0 for dp in data_points)
                    out[key] = int(total)
                elif hasattr(data_points[0], "sum"):
                    # Histogram: sum of durations (sum of data points)
                    total_sum = sum(getattr(dp, "sum", 0) or 0 for dp in data_points)
                    out[key] = round(float(total_sum), 2)
    return out

# References for shutdown / flush
_meter_provider: Optional[MeterProvider] = None
_metric_reader: Optional[PeriodicExportingMetricReader] = None
_instruments: Optional[dict] = None


def init_metrics(
    service_name: str = "fsudaemon",
    service_version: str = "1.0.0",
    export_interval_seconds: int = 60,
) -> None:
    """
    Configure OpenTelemetry with metrics export to stdout.

    Args:
        service_name: Service name
        service_version: Service version
        export_interval_seconds: Export interval in seconds (default 60)
    """
    global _meter_provider, _metric_reader, _instruments

    exporter = StructlogMetricExporter()
    _metric_reader = PeriodicExportingMetricReader(
        exporter,
        export_interval_millis=export_interval_seconds * 1000,
    )
    _meter_provider = MeterProvider(metric_readers=[_metric_reader])
    metrics.set_meter_provider(_meter_provider)

    meter = _meter_provider.get_meter(service_name, service_version)

    # Create instruments once
    _instruments = {
        "messages_received": meter.create_counter(
            "fsudaemon.messages_received",
            description="Number of SQS messages received",
        ),
        "messages_processed": meter.create_counter(
            "fsudaemon.messages_processed",
            description="Number of messages processed successfully (with DB insert)",
        ),
        "events_kept": meter.create_counter(
            "fsudaemon.events_kept",
            description="Number of S3 events kept (match audit point)",
        ),
        "events_discarded": meter.create_counter(
            "fsudaemon.events_discarded",
            description="Number of S3 events discarded (no match)",
        ),
        "total_associations": meter.create_counter(
            "fsudaemon.total_associations",
            description="Total number of event / audit point associations",
        ),
        "errors": meter.create_counter(
            "fsudaemon.errors",
            description="Number of processing errors",
        ),
        "db_insert_seconds": meter.create_histogram(
            "fsudaemon.db_insert_seconds",
            description="DB insert duration in seconds",
            unit="s",
        ),
        "sqs_receive_message_seconds": meter.create_histogram(
            "fsudaemon.sqs_receive_message_seconds",
            description="SQS receive_message call duration in seconds",
            unit="s",
        ),
        "sqs_delete_message_batch_seconds": meter.create_histogram(
            "fsudaemon.sqs_delete_message_batch_seconds",
            description="SQS delete_message_batch call duration in seconds",
            unit="s",
        ),
    }


def instruments() -> Dict[str, Any]:
    """Return instruments to record metrics (after init_metrics())."""
    if _instruments is None:
        raise RuntimeError("Telemetry not initialized. Call init_metrics() first.")
    return _instruments


def shutdown_metrics() -> None:
    """Flush and graceful shutdown of MeterProvider (call on exit)."""
    global _metric_reader, _meter_provider, _instruments
    _instruments = None
    if _metric_reader is not None:
        try:
            _metric_reader.force_flush()
        except Exception:
            pass
    if _meter_provider is not None:
        try:
            _meter_provider.shutdown()
        except Exception:
            pass
        _meter_provider = None
    _metric_reader = None
