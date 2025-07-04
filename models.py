"""
Data models for Snowflake DataHub Connector

This module defines the data models used for metadata representation
and ingestion results.
"""

from dataclasses import dataclass, field
from typing import List, Optional, Dict, Any
from datetime import datetime


@dataclass
class FieldMetadata:
    """Metadata for a database field/column."""
    name: str
    type: str
    nullable: bool = True
    description: Optional[str] = None
    ordinal_position: Optional[int] = None
    default_value: Optional[str] = None
    is_primary_key: bool = False
    is_foreign_key: bool = False


@dataclass
class SchemaMetadata:
    """Metadata for a database schema."""
    name: str
    fields: List[FieldMetadata] = field(default_factory=list)
    primary_keys: List[str] = field(default_factory=list)
    foreign_keys: List[Dict[str, Any]] = field(default_factory=list)


@dataclass
class DatasetMetadata:
    """Metadata for a dataset (table/view)."""
    name: str
    platform: str
    database: str
    schema: str
    table_type: str = "TABLE"
    description: Optional[str] = None
    schema_metadata: Optional[SchemaMetadata] = None
    row_count: Optional[int] = None
    size_bytes: Optional[int] = None
    created_at: Optional[datetime] = None
    last_modified: Optional[datetime] = None
    owner: Optional[str] = None
    tags: List[str] = field(default_factory=list)
    custom_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class UserMetadata:
    """Metadata for a user."""
    name: str
    email: Optional[str] = None
    display_name: Optional[str] = None
    is_active: bool = True
    roles: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    last_login: Optional[datetime] = None
    custom_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class GroupMetadata:
    """Metadata for a group/role."""
    name: str
    description: Optional[str] = None
    is_active: bool = True
    members: List[str] = field(default_factory=list)
    created_at: Optional[datetime] = None
    owner: Optional[str] = None
    custom_properties: Dict[str, str] = field(default_factory=dict)


@dataclass
class IngestionResult:
    """Result of metadata ingestion process."""
    status: str = "in_progress"
    duration: float = 0.0
    
    # Dataset statistics
    datasets_processed: int = 0
    datasets_success: int = 0
    datasets_failed: int = 0
    
    # User statistics
    users_processed: int = 0
    users_success: int = 0
    users_failed: int = 0
    
    # Group statistics
    groups_processed: int = 0
    groups_success: int = 0
    groups_failed: int = 0
    
    # Errors
    errors: List[str] = field(default_factory=list)
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert result to dictionary."""
        return {
            'status': self.status,
            'duration': self.duration,
            'datasets': {
                'processed': self.datasets_processed,
                'success': self.datasets_success,
                'failed': self.datasets_failed
            },
            'users': {
                'processed': self.users_processed,
                'success': self.users_success,
                'failed': self.users_failed
            },
            'groups': {
                'processed': self.groups_processed,
                'success': self.groups_success,
                'failed': self.groups_failed
            },
            'errors': self.errors
        }
    
    def get_summary(self) -> str:
        """Get a summary string of the ingestion results."""
        total_processed = self.datasets_processed + self.users_processed + self.groups_processed
        total_success = self.datasets_success + self.users_success + self.groups_success
        total_failed = self.datasets_failed + self.users_failed + self.groups_failed
        
        summary = f"Ingestion {self.status} in {self.duration:.2f}s: "
        summary += f"{total_success}/{total_processed} successful"
        
        if total_failed > 0:
            summary += f", {total_failed} failed"
        
        return summary


@dataclass
class ConnectionTest:
    """Result of connection test."""
    snowflake_connected: bool = False
    snowflake_error: Optional[str] = None
    datahub_connected: bool = False
    datahub_error: Optional[str] = None
    permissions: Dict[str, bool] = field(default_factory=dict)
    
    def is_ready(self) -> bool:
        """Check if both connections are ready."""
        return self.snowflake_connected and self.datahub_connected
    
    def get_issues(self) -> List[str]:
        """Get list of connection issues."""
        issues = []
        
        if not self.snowflake_connected:
            issues.append(f"Snowflake connection failed: {self.snowflake_error}")
        
        if not self.datahub_connected:
            issues.append(f"DataHub connection failed: {self.datahub_error}")
        
        return issues


@dataclass
class MetadataExtractorStats:
    """Statistics for metadata extraction."""
    databases_found: int = 0
    schemas_found: int = 0
    tables_found: int = 0
    views_found: int = 0
    users_found: int = 0
    roles_found: int = 0
    extraction_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            'databases': self.databases_found,
            'schemas': self.schemas_found,
            'tables': self.tables_found,
            'views': self.views_found,
            'users': self.users_found,
            'roles': self.roles_found,
            'extraction_time': self.extraction_time
        }


@dataclass
class DataHubIngestionStats:
    """Statistics for DataHub ingestion."""
    entities_sent: int = 0
    entities_success: int = 0
    entities_failed: int = 0
    batches_sent: int = 0
    ingestion_time: float = 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert stats to dictionary."""
        return {
            'entities_sent': self.entities_sent,
            'entities_success': self.entities_success,
            'entities_failed': self.entities_failed,
            'batches_sent': self.batches_sent,
            'ingestion_time': self.ingestion_time
        }


@dataclass
class ConnectorRunMetrics:
    """Comprehensive metrics for a connector run."""
    start_time: datetime
    end_time: Optional[datetime] = None
    extraction_stats: Optional[MetadataExtractorStats] = None
    ingestion_stats: Optional[DataHubIngestionStats] = None
    result: Optional[IngestionResult] = None
    
    def get_duration(self) -> float:
        """Get total duration of the run."""
        if self.end_time:
            return (self.end_time - self.start_time).total_seconds()
        return 0.0
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert metrics to dictionary."""
        return {
            'start_time': self.start_time.isoformat(),
            'end_time': self.end_time.isoformat() if self.end_time else None,
            'duration': self.get_duration(),
            'extraction_stats': self.extraction_stats.to_dict() if self.extraction_stats else None,
            'ingestion_stats': self.ingestion_stats.to_dict() if self.ingestion_stats else None,
            'result': self.result.to_dict() if self.result else None
        }
