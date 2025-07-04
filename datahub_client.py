"""
DataHub Client for metadata ingestion

This module provides functionality to ingest metadata into DataHub
via REST API calls.
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from config import DataHubConfig
from models import DatasetMetadata, UserMetadata, GroupMetadata

logger = logging.getLogger(__name__)


class DataHubClient:
    """Client for DataHub metadata ingestion."""
    
    def __init__(self, config: DataHubConfig, platform: str = "snowflake"):
        """Initialize the DataHub client."""
        self.config = config
        self.platform = platform
        self.session = self._create_session()
        self.base_url = self.config.server_url
        
    def _create_session(self) -> requests.Session:
        """Create a requests session with retry strategy."""
        session = requests.Session()
        
        # Set up retry strategy
        retry_strategy = Retry(
            total=self.config.max_retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE", "POST"]
        )
        
        adapter = HTTPAdapter(max_retries=retry_strategy)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        
        # Set headers
        session.headers.update({
            'Content-Type': 'application/json',
            'User-Agent': 'snowflake-datahub-connector/1.0'
        })
        
        # Add authentication if token is provided
        if self.config.token:
            session.headers.update({
                'Authorization': f'Bearer {self.config.token}'
            })
        
        return session
    
    def test_connection(self) -> bool:
        """Test connection to DataHub."""
        try:
            logger.info("Testing DataHub connection")
            
            # Try to get DataHub config
            url = urljoin(self.base_url, '/config')
            response = self.session.get(url, timeout=self.config.timeout)
            
            if response.status_code == 200:
                logger.info("Successfully connected to DataHub")
                return True
            else:
                logger.warning(f"DataHub connection test returned status: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"DataHub connection test failed: {e}")
            return False
    
    def ingest_dataset(self, dataset: DatasetMetadata) -> None:
        """
        Ingest dataset metadata into DataHub.
        
        Args:
            dataset: Dataset metadata to ingest
        """
        try:
            logger.debug(f"Ingesting dataset: {dataset.name}")
            
            # Create dataset URN
            dataset_urn = self._create_dataset_urn(dataset)
            
            # Create metadata change events
            events = []
            
            # Dataset properties
            dataset_properties = {
                'description': dataset.description,
                'customProperties': {
                    'platform': dataset.platform,
                    'database': dataset.database,
                    'schema': dataset.schema,
                    'table_type': dataset.table_type,
                    'owner': dataset.owner,
                }
            }
            
            if dataset.row_count is not None:
                dataset_properties['customProperties']['row_count'] = str(dataset.row_count)
            if dataset.size_bytes is not None:
                dataset_properties['customProperties']['size_bytes'] = str(dataset.size_bytes)
            if dataset.created_at is not None:
                dataset_properties['customProperties']['created_at'] = str(dataset.created_at)
            if dataset.last_modified is not None:
                dataset_properties['customProperties']['last_modified'] = str(dataset.last_modified)
            
            # Dataset properties event
            events.append({
                'auditHeader': self._create_audit_header(),
                'entityType': 'dataset',
                'entityUrn': dataset_urn,
                'changeType': 'UPSERT',
                'aspectName': 'datasetProperties',
                'aspect': {
                    'value': json.dumps(dataset_properties)
                }
            })
            
            # Schema metadata
            if dataset.schema_metadata:
                schema_metadata = self._create_schema_metadata(dataset.schema_metadata)
                events.append({
                    'auditHeader': self._create_audit_header(),
                    'entityType': 'dataset',
                    'entityUrn': dataset_urn,
                    'changeType': 'UPSERT',
                    'aspectName': 'schemaMetadata',
                    'aspect': {
                        'value': json.dumps(schema_metadata)
                    }
                })
            
            # Send events
            self._send_metadata_events(events)
            logger.debug(f"Successfully ingested dataset: {dataset.name}")
            
        except Exception as e:
            logger.error(f"Error ingesting dataset {dataset.name}: {e}")
            raise
    
    def ingest_user(self, user: UserMetadata) -> None:
        """
        Ingest user metadata into DataHub.
        
        Args:
            user: User metadata to ingest
        """
        try:
            logger.debug(f"Ingesting user: {user.name}")
            
            # Create user URN
            user_urn = self._create_user_urn(user)
            
            # Create platform user properties
            user_properties = {
                'platform': self.platform,
                'username': user.name,
                'email': user.email,
                'displayName': user.display_name,
                'active': user.is_active,
                'roles': user.roles,
                'createdAt': user.created_at.isoformat() if user.created_at else None,
                'lastLogin': user.last_login.isoformat() if user.last_login else None,
                'customProperties': user.custom_properties
            }
            
            # Create metadata change event
            event = {
                'auditHeader': self._create_audit_header(),
                'entityType': 'platformUser',
                'entityUrn': user_urn,
                'changeType': 'UPSERT',
                'aspectName': 'platformUserInfo',
                'aspect': {
                    'value': json.dumps(user_properties)
                }
            }
            
            self._send_metadata_events([event])
            logger.debug(f"Successfully ingested user: {user.name}")
            
        except Exception as e:
            logger.error(f"Error ingesting user {user.name}: {e}")
            raise
    
    def ingest_group(self, group: GroupMetadata) -> None:
        """
        Ingest group metadata into DataHub.
        
        Args:
            group: Group metadata to ingest
        """
        try:
            logger.debug(f"Ingesting group: {group.name}")
            
            # Create group URN
            group_urn = self._create_group_urn(group)
            
            # Create platform user group properties
            group_properties = {
                'platform': self.platform,
                'groupName': group.name,
                'displayName': group.name,
                'description': group.description,
                'active': group.is_active,
                'members': group.members,
                'owner': group.owner,
                'createdAt': group.created_at.isoformat() if group.created_at else None,
                'customProperties': group.custom_properties
            }
            
            # Create metadata change event
            event = {
                'auditHeader': self._create_audit_header(),
                'entityType': 'platformUserGroup',
                'entityUrn': group_urn,
                'changeType': 'UPSERT',
                'aspectName': 'platformUserGroupInfo',
                'aspect': {
                    'value': json.dumps(group_properties)
                }
            }
            
            self._send_metadata_events([event])
            logger.debug(f"Successfully ingested group: {group.name}")
            
        except Exception as e:
            logger.error(f"Error ingesting group {group.name}: {e}")
            raise
    
    def _create_dataset_urn(self, dataset: DatasetMetadata) -> str:
        """Create dataset URN."""
        return f"urn:li:dataset:(urn:li:dataPlatform:{dataset.platform},{dataset.database}.{dataset.schema}.{dataset.name},PROD)"
    
    def _create_user_urn(self, user: UserMetadata) -> str:
        """Create platform user URN."""
        return f"urn:li:platformUser:(urn:li:dataPlatform:{self.platform},{user.name})"
    
    def _create_group_urn(self, group: GroupMetadata) -> str:
        """Create platform user group URN."""
        return f"urn:li:platformUserGroup:(urn:li:dataPlatform:{self.platform},{group.name})"
    
    def _create_audit_header(self) -> Dict[str, Any]:
        """Create audit header for metadata events."""
        return {
            'time': int(time.time() * 1000),
            'actor': 'urn:li:corpuser:snowflake-connector',
            'impersonator': None
        }
    
    def _create_schema_metadata(self, schema_metadata) -> Dict[str, Any]:
        """Create schema metadata for DataHub."""
        fields = []
        
        for field in schema_metadata.fields:
            field_data = {
                'fieldPath': field.name,
                'nativeDataType': field.type,
                'type': {
                    'type': {
                        'com.linkedin.pegasus2avro.schema.StringType': {}
                    }
                },
                'description': field.description,
                'nullable': field.nullable,
                'recursive': False
            }
            fields.append(field_data)
        
        return {
            'schemaName': schema_metadata.name,
            'platform': 'urn:li:dataPlatform:snowflake',
            'version': 0,
            'fields': fields,
            'platformSchema': {
                'com.linkedin.pegasus2avro.schema.MySqlDDL': {
                    'tableSchema': ''
                }
            }
        }
    
    def _send_metadata_events(self, events: List[Dict[str, Any]]) -> None:
        """Send metadata events to DataHub."""
        try:
            # Send events in batches
            batch_size = self.config.batch_size
            
            for i in range(0, len(events), batch_size):
                batch = events[i:i + batch_size]
                
                # Send batch
                url = urljoin(self.base_url, '/entities?action=ingest')
                response = self.session.post(
                    url,
                    json={'elements': batch},
                    timeout=self.config.timeout
                )
                
                if response.status_code not in [200, 201]:
                    logger.error(f"DataHub ingestion failed: {response.status_code} - {response.text}")
                    response.raise_for_status()
                
                logger.debug(f"Successfully sent batch of {len(batch)} events to DataHub")
                
                # Rate limiting
                time.sleep(0.1)
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error sending metadata events to DataHub: {e}")
            raise
    
    def get_entity(self, urn: str) -> Optional[Dict[str, Any]]:
        """Get entity from DataHub by URN."""
        try:
            url = urljoin(self.base_url, f'/entities/{urn}')
            response = self.session.get(url, timeout=self.config.timeout)
            
            if response.status_code == 200:
                return response.json()
            elif response.status_code == 404:
                return None
            else:
                response.raise_for_status()
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error getting entity {urn} from DataHub: {e}")
            raise
    
    def delete_entity(self, urn: str) -> bool:
        """Delete entity from DataHub."""
        try:
            url = urljoin(self.base_url, f'/entities/{urn}')
            response = self.session.delete(url, timeout=self.config.timeout)
            
            if response.status_code in [200, 204]:
                logger.debug(f"Successfully deleted entity: {urn}")
                return True
            else:
                logger.error(f"Failed to delete entity {urn}: {response.status_code}")
                return False
                
        except requests.exceptions.RequestException as e:
            logger.error(f"Error deleting entity {urn} from DataHub: {e}")
            return False
    
    def health_check(self) -> Dict[str, Any]:
        """Check DataHub health status."""
        try:
            url = urljoin(self.base_url, '/health')
            response = self.session.get(url, timeout=self.config.timeout)
            
            if response.status_code == 200:
                return response.json()
            else:
                return {'status': 'unhealthy', 'code': response.status_code}
                
        except requests.exceptions.RequestException as e:
            logger.error(f"DataHub health check failed: {e}")
            return {'status': 'error', 'error': str(e)}
