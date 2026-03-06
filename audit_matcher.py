"""
Audit Point Matcher - Fast matching of S3 events against audit points
"""
from typing import List, Dict, Tuple
from models import AuditPoint
import structlog

logger = structlog.get_logger(__name__)


class AuditPointMatcher:
    """
    Matcher to determine which audit points match an S3 event.
    
    For hierarchical prefixes like /data, /data/2024, /data/2024/dir1,
    an object /data/2024/dir1/file.txt will match all 3 audit points.
    """
    
    def __init__(self, audit_points: List[AuditPoint] = None):
        """
        Initializes the matcher with a list of audit points.
        
        Args:
            audit_points: List of audit points to load
        """
        # Structure: dict[bucket] → list[(audit_point_id, prefix)]
        self.audit_points: Dict[str, List[Tuple[int, str]]] = {}
        self._total_audit_points = 0
        
        if audit_points:
            self.load_audit_points(audit_points)
    
    def load_audit_points(self, audit_points: List[AuditPoint]) -> None:
        """
        Loads audit points into the matching structure.
        
        Args:
            audit_points: List of audit points to load
        """
        self.audit_points.clear()
        
        for ap in audit_points:
            if ap.bucket not in self.audit_points:
                self.audit_points[ap.bucket] = []
            
            self.audit_points[ap.bucket].append((ap.id, ap.prefix))
        
        # Sort by prefix length (longest first) for optimization
        for bucket in self.audit_points:
            self.audit_points[bucket].sort(key=lambda x: len(x[1]), reverse=True)
        
        self._total_audit_points = len(audit_points)
        
        logger.debug(
            "audit_points_loaded",
            total=self._total_audit_points,
            buckets=len(self.audit_points)
        )
    
    def get_matching_audit_points(self, bucket: str, key: str) -> List[int]:
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
        if bucket not in self.audit_points:
            return []
        
        matching_ids = []
        
        for audit_point_id, prefix in self.audit_points[bucket]:
            if key.startswith(prefix):
                matching_ids.append(audit_point_id)
        
        return matching_ids
    
    def matches(self, bucket: str, key: str) -> bool:
        """
        Returns True if at least one audit point matches.
        
        Args:
            bucket: S3 bucket name
            key: S3 object key
        
        Returns:
            True if at least one audit point matches
        """
        return len(self.get_matching_audit_points(bucket, key)) > 0
    
    def get_stats(self) -> Dict:
        """Returns statistics about loaded audit points"""
        total_prefixes = sum(len(prefixes) for prefixes in self.audit_points.values())
        
        return {
            "total_audit_points": self._total_audit_points,
            "buckets": len(self.audit_points),
            "total_prefixes": total_prefixes,
            "buckets_detail": {
                bucket: len(prefixes) 
                for bucket, prefixes in self.audit_points.items()
            }
        }
