"""
Data models for S3 Event Processor
"""
from datetime import datetime
from enum import Enum
from typing import Optional

from pydantic import BaseModel, ConfigDict, Field


class S3EventName(str, Enum):
    """
    AWS S3 event names as documented in:
    https://docs.aws.amazon.com/AmazonS3/latest/userguide/notification-how-to-event-types-and-destinations.html
    """
    # Object created
    OBJECT_CREATED_PUT = "ObjectCreated:Put"
    OBJECT_CREATED_POST = "ObjectCreated:Post"
    OBJECT_CREATED_COPY = "ObjectCreated:Copy"
    OBJECT_CREATED_COMPLETE_MULTIPART_UPLOAD = "ObjectCreated:CompleteMultipartUpload"
    # Object removed
    OBJECT_REMOVED_DELETE = "ObjectRemoved:Delete"
    OBJECT_REMOVED_DELETE_MARKER_CREATED = "ObjectRemoved:DeleteMarkerCreated"
    # Object restore (from Glacier)
    OBJECT_RESTORE_POST = "ObjectRestore:Post"
    OBJECT_RESTORE_COMPLETED = "ObjectRestore:Completed"
    OBJECT_RESTORE_DELETE = "ObjectRestore:Delete"
    # Reduced redundancy storage lost
    REDUCED_REDUNDANCY_LOST_OBJECT = "ReducedRedundancyLostObject"
    # Replication
    REPLICATION_OPERATION_FAILED_REPLICATION = "Replication:OperationFailedReplication"
    REPLICATION_OPERATION_MISSED_THRESHOLD = "Replication:OperationMissedThreshold"
    REPLICATION_OPERATION_REPLICATED_AFTER_THRESHOLD = "Replication:OperationReplicatedAfterThreshold"
    REPLICATION_OPERATION_NOT_TRACKED = "Replication:OperationNotTracked"
    # Object tagging
    OBJECT_TAGGING_PUT = "ObjectTagging:Put"
    OBJECT_TAGGING_DELETE = "ObjectTagging:Delete"
    # Object ACL
    OBJECT_ACL_PUT = "ObjectAcl:Put"
    # Lifecycle
    LIFECYCLE_EXPIRATION_DELETE = "LifecycleExpiration:Delete"
    LIFECYCLE_EXPIRATION_DELETE_MARKER_CREATED = "LifecycleExpiration:DeleteMarkerCreated"
    LIFECYCLE_TRANSITION = "LifecycleTransition"
    # Intelligent tiering
    INTELLIGENT_TIERING = "IntelligentTiering"


class AuditPoint(BaseModel):
    """Represents an audit point (bucket + prefix)"""

    model_config = ConfigDict(frozen=True)

    id: int
    bucket: str
    prefix: str
    description: Optional[str] = None
    created_at: Optional[datetime] = None
    deleted_at: Optional[datetime] = None

    def __repr__(self) -> str:
        status = " (deleted)" if self.deleted_at else ""
        return f"AuditPoint(id={self.id}, bucket='{self.bucket}', prefix='{self.prefix}'{status})"


class S3Event(BaseModel):
    """Represents an S3 event"""

    model_config = ConfigDict(frozen=True)

    event_time: datetime
    event_name: S3EventName
    bucket: str
    object_key: str
    size: int = Field(ge=0)  # enforced at Python level, mirrors CHECK (size >= 0) in DB
    version_id: Optional[str] = None
    audit_point_ids: list[int] = Field(default_factory=list)

    def __repr__(self) -> str:
        return (f"S3Event(bucket='{self.bucket}', key='{self.object_key}', "
                f"event='{self.event_name.value}', audit_points={len(self.audit_point_ids)})")
