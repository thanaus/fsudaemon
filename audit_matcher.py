"""
Audit Point Matcher - Fast matching of S3 events against audit points
"""
from models import AuditPoint
import structlog

logger = structlog.get_logger(__name__)


class AuditPointMatcher:
    """
    Matcher to determine which audit points match an S3 event.

    For hierarchical prefixes like /data, /data/2024, /data/2024/dir1,
    an object /data/2024/dir1/file.txt will match all 3 audit points.
    """

    _audit_points: dict[str, list[tuple[int, str]]]
    _total_audit_points: int

    def __init__(self, audit_points: list[AuditPoint] | None = None):
        """
        Initializes the matcher with a list of audit points.

        Args:
            audit_points: List of audit points to load
        """
        # Structure: dict[bucket] → list[(audit_point_id, prefix)]
        self._audit_points = {}
        self._total_audit_points = 0

        if audit_points:
            self.load_audit_points(audit_points)

    def load_audit_points(self, audit_points: list[AuditPoint]) -> None:
        """
        Loads audit points into the matching structure.

        Args:
            audit_points: List of audit points to load
        """
        new_audit_points: dict[str, list[tuple[int, str]]] = {}

        for ap in audit_points:
            if not ap.bucket:
                raise ValueError(f"AuditPoint id={ap.id} has an invalid bucket: {ap.bucket!r}")
            if ap.prefix is None:
                raise ValueError(f"AuditPoint id={ap.id} has a None prefix")

            if ap.bucket not in new_audit_points:
                new_audit_points[ap.bucket] = []

            # Normalize prefix: ensure it ends with '/' unless empty (empty = match entire bucket)
            prefix = ap.prefix
            if prefix and not prefix.endswith("/"):
                prefix += "/"

            new_audit_points[ap.bucket].append((ap.id, prefix))

        # Atomic assignment — no intermediate empty state visible to concurrent readers
        # (safe under asyncio single-thread execution)
        self._audit_points = new_audit_points

        self._total_audit_points = len(audit_points)

        logger.debug(
            "audit_points_loaded",
            total=self._total_audit_points,
            buckets=len(self._audit_points)
        )

    def get_matching_audit_points(self, bucket: str, key: str) -> list[int]:
        """
        Returns the list of ALL audit point IDs that match an event.

        For an object /data/2024/dir1/file.txt with audit points:
        - /data
        - /data/2024
        - /data/2024/dir1

        Will return all 3 IDs.

        Args:
            bucket: S3 bucket name
            key: S3 object key

        Returns:
            List of matching audit_point_ids (can be empty)
        """
        if bucket not in self._audit_points:
            return []

        matching_ids = []

        for audit_point_id, prefix in self._audit_points[bucket]:
            if key.startswith(prefix):
                matching_ids.append(audit_point_id)

        return matching_ids

    def get_stats(self) -> dict:
        """Returns statistics about loaded audit points"""
        total_prefixes = sum(len(prefixes) for prefixes in self._audit_points.values())

        return {
            "total_audit_points": self._total_audit_points,
            "buckets": len(self._audit_points),
            "total_prefixes": total_prefixes,
            "buckets_detail": {
                bucket: len(prefixes)
                for bucket, prefixes in self._audit_points.items()
            }
        }
