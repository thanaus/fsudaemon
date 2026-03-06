"""
Data models for S3 Event Processor
"""
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional, List


@dataclass
class AuditPoint:
    """Represents an audit point (bucket + prefix)"""
    id: int
    bucket: str
    prefix: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None
    
    def __repr__(self) -> str:
        status = " (deleted)" if self.deleted_at else ""
        return f"AuditPoint(id={self.id}, bucket='{self.bucket}', prefix='{self.prefix}'{status})"


@dataclass
class S3Event:
    """Represents an S3 event"""
    event_time: datetime
    event_name: str
    bucket: str
    object_key: str
    size: int
    version_id: Optional[str] = None
    audit_point_ids: List[int] = field(default_factory=list)
    
    def __repr__(self) -> str:
        return (f"S3Event(bucket='{self.bucket}', key='{self.object_key}', "
                f"event='{self.event_name}', audit_points={len(self.audit_point_ids)})")
