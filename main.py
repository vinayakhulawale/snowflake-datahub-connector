#!/usr/bin/env python3
"""
Snowflake to DataHub Metadata Ingestion Connector

This module provides the main entry point for the Snowflake metadata connector
that extracts metadata from Snowflake and ingests it into DataHub.
"""

import logging
import sys
from typing import Dict, List, Optional
from datetime import datetime

from snowflake_connector import SnowflakeConnector
from datahub_client import DataHubClient
from config import Config
from models import (
    DatasetMetadata, SchemaMetadata, FieldMetadata, 
    UserMetadata, GroupMetadata, IngestionResult
)
from utils import setup_logging, retry_with_backoff

logger = logging.getLogger(__name__)


class SnowflakeDataHubConnector:
    """Main connector class for Snowflake to DataHub metadata ingestion."""
    
    def __init__(self, config: Config):
        """Initialize the connector with configuration."""
        self.config = config
        self.snowflake_connector = SnowflakeConnector(config.snowflake)
        self.datahub_client = DataHubClient(config.datahub, platform=config.platform)
        
    def extract_and_ingest_metadata(self) -> IngestionResult:
        """
        Extract metadata from Snowflake and ingest into DataHub.
        
        Returns:
            IngestionResult: Summary of the ingestion process
        """
        result = IngestionResult()
        start_time = datetime.now()
        
        try:
            logger.info("Starting Snowflake metadata extraction and DataHub ingestion")
            
            # Connect to Snowflake
            logger.info("Connecting to Snowflake")
            self.snowflake_connector.connect()
            
            # Extract structural metadata
            if self.config.extract_structural_metadata:
                logger.info("Extracting structural metadata")
                datasets = self._extract_datasets()
                result.datasets_processed = len(datasets)
                
                # Ingest datasets
                for dataset in datasets:
                    try:
                        self._ingest_dataset(dataset)
                        result.datasets_success += 1
                    except Exception as e:
                        logger.error(f"Failed to ingest dataset {dataset.name}: {e}")
                        result.datasets_failed += 1
                        result.errors.append(f"Dataset {dataset.name}: {str(e)}")
            
            # Extract access control metadata
            if self.config.extract_access_control:
                logger.info("Extracting access control metadata")
                
                # Extract users
                users = self._extract_users()
                result.users_processed = len(users)
                
                for user in users:
                    try:
                        self._ingest_user(user)
                        result.users_success += 1
                    except Exception as e:
                        logger.error(f"Failed to ingest user {user.name}: {e}")
                        result.users_failed += 1
                        result.errors.append(f"User {user.name}: {str(e)}")
                
                # Extract groups
                groups = self._extract_groups()
                result.groups_processed = len(groups)
                
                for group in groups:
                    try:
                        self._ingest_group(group)
                        result.groups_success += 1
                    except Exception as e:
                        logger.error(f"Failed to ingest group {group.name}: {e}")
                        result.groups_failed += 1
                        result.errors.append(f"Group {group.name}: {str(e)}")
            
            result.status = "completed"
            result.duration = (datetime.now() - start_time).total_seconds()
            
            logger.info(f"Ingestion completed successfully in {result.duration:.2f} seconds")
            logger.info(f"Summary: {result.datasets_success}/{result.datasets_processed} datasets, "
                       f"{result.users_success}/{result.users_processed} users, "
                       f"{result.groups_success}/{result.groups_processed} groups")
            
        except Exception as e:
            result.status = "failed"
            result.duration = (datetime.now() - start_time).total_seconds()
            result.errors.append(f"Critical error: {str(e)}")
            logger.error(f"Ingestion failed: {e}")
            raise
        
        finally:
            # Clean up connections
            try:
                self.snowflake_connector.disconnect()
            except Exception as e:
                logger.warning(f"Error disconnecting from Snowflake: {e}")
        
        return result
    
    def _extract_datasets(self) -> List[DatasetMetadata]:
        """Extract dataset metadata from Snowflake."""
        datasets = []
        
        try:
            # Get all databases
            databases = self.snowflake_connector.get_databases()
            
            for db in databases:
                if self._should_include_database(db):
                    # Get schemas in database
                    schemas = self.snowflake_connector.get_schemas(db)
                    
                    for schema in schemas:
                        if self._should_include_schema(schema):
                            # Get tables and views in schema
                            tables = self.snowflake_connector.get_tables(db, schema)
                            
                            for table in tables:
                                if self._should_include_table(table['TABLE_NAME']):
                                    dataset = self._build_dataset_metadata(db, schema, table)
                                    datasets.append(dataset)
        
        except Exception as e:
            logger.error(f"Error extracting datasets: {e}")
            raise
        
        return datasets
    
    def _extract_users(self) -> List[UserMetadata]:
        """Extract user metadata from Snowflake."""
        try:
            users_data = self.snowflake_connector.get_users()
            users = []
            
            for user_data in users_data:
                user = UserMetadata(
                    name=user_data['NAME'],
                    email=user_data.get('EMAIL'),
                    display_name=user_data.get('DISPLAY_NAME', user_data['NAME']),
                    is_active=not user_data.get('DISABLED', False),
                    roles=user_data.get('ROLES', []),
                    created_at=user_data.get('CREATED_ON'),
                    last_login=user_data.get('LAST_SUCCESS_LOGIN')
                )
                users.append(user)
            
            return users
        
        except Exception as e:
            logger.error(f"Error extracting users: {e}")
            raise
    
    def _extract_groups(self) -> List[GroupMetadata]:
        """Extract group/role metadata from Snowflake."""
        try:
            groups_data = self.snowflake_connector.get_roles()
            groups = []
            
            for group_data in groups_data:
                group = GroupMetadata(
                    name=group_data['NAME'],
                    description=group_data.get('COMMENT'),
                    is_active=True,  # Roles are typically active if they exist
                    members=group_data.get('MEMBERS', []),
                    created_at=group_data.get('CREATED_ON'),
                    owner=group_data.get('OWNER')
                )
                groups.append(group)
            
            return groups
        
        except Exception as e:
            logger.error(f"Error extracting groups: {e}")
            raise
    
    def _build_dataset_metadata(self, database: str, schema: str, table: Dict) -> DatasetMetadata:
        """Build dataset metadata from Snowflake table information."""
        table_name = table['TABLE_NAME']
        
        # Get column information
        columns = self.snowflake_connector.get_table_columns(database, schema, table_name)
        
        # Build field metadata
        fields = []
        for col in columns:
            field = FieldMetadata(
                name=col['COLUMN_NAME'],
                type=col['DATA_TYPE'],
                nullable=col.get('IS_NULLABLE', 'YES') == 'YES',
                description=col.get('COMMENT'),
                ordinal_position=col.get('ORDINAL_POSITION'),
                default_value=col.get('COLUMN_DEFAULT')
            )
            fields.append(field)
        
        # Build schema metadata
        schema_metadata = SchemaMetadata(
            name=f"{database}.{schema}",
            fields=fields,
            primary_keys=self._get_primary_keys(database, schema, table_name),
            foreign_keys=self._get_foreign_keys(database, schema, table_name)
        )
        
        # Build dataset metadata
        dataset = DatasetMetadata(
            name=table_name,
            platform="snowflake",
            database=database,
            schema=schema,
            table_type=table.get('TABLE_TYPE', 'TABLE'),
            description=table.get('COMMENT'),
            schema_metadata=schema_metadata,
            row_count=table.get('ROW_COUNT'),
            size_bytes=table.get('BYTES'),
            created_at=table.get('CREATED'),
            last_modified=table.get('LAST_ALTERED'),
            owner=table.get('TABLE_OWNER')
        )
        
        return dataset
    
    def _get_primary_keys(self, database: str, schema: str, table: str) -> List[str]:
        """Get primary key columns for a table."""
        try:
            return self.snowflake_connector.get_primary_keys(database, schema, table)
        except Exception as e:
            logger.warning(f"Could not get primary keys for {database}.{schema}.{table}: {e}")
            return []
    
    def _get_foreign_keys(self, database: str, schema: str, table: str) -> List[Dict]:
        """Get foreign key constraints for a table."""
        try:
            return self.snowflake_connector.get_foreign_keys(database, schema, table)
        except Exception as e:
            logger.warning(f"Could not get foreign keys for {database}.{schema}.{table}: {e}")
            return []
    
    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def _ingest_dataset(self, dataset: DatasetMetadata):
        """Ingest dataset metadata into DataHub."""
        self.datahub_client.ingest_dataset(dataset)
    
    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def _ingest_user(self, user: UserMetadata):
        """Ingest user metadata into DataHub."""
        self.datahub_client.ingest_user(user)
    
    @retry_with_backoff(max_retries=3, backoff_factor=2)
    def _ingest_group(self, group: GroupMetadata):
        """Ingest group metadata into DataHub."""
        self.datahub_client.ingest_group(group)
    
    def _should_include_database(self, database: str) -> bool:
        """Check if database should be included based on configuration."""
        if self.config.include_databases:
            return database in self.config.include_databases
        if self.config.exclude_databases:
            return database not in self.config.exclude_databases
        return True
    
    def _should_include_schema(self, schema: str) -> bool:
        """Check if schema should be included based on configuration."""
        if self.config.include_schemas:
            return schema in self.config.include_schemas
        if self.config.exclude_schemas:
            return schema not in self.config.exclude_schemas
        return True
    
    def _should_include_table(self, table: str) -> bool:
        """Check if table should be included based on configuration."""
        if self.config.include_tables:
            return table in self.config.include_tables
        if self.config.exclude_tables:
            return table not in self.config.exclude_tables
        return True


def main():
    """Main entry point for the connector."""
    try:
        # Setup logging
        setup_logging()
        
        # Load configuration
        config = Config.load_config()
        
        # Create and run connector
        connector = SnowflakeDataHubConnector(config)
        result = connector.extract_and_ingest_metadata()
        
        # Print summary
        print(f"\nIngestion Summary:")
        print(f"Status: {result.status}")
        print(f"Duration: {result.duration:.2f} seconds")
        print(f"Datasets: {result.datasets_success}/{result.datasets_processed}")
        print(f"Users: {result.users_success}/{result.users_processed}")
        print(f"Groups: {result.groups_success}/{result.groups_processed}")
        
        if result.errors:
            print(f"\nErrors ({len(result.errors)}):")
            for error in result.errors:
                print(f"  - {error}")
        
        # Exit with appropriate code
        sys.exit(0 if result.status == "completed" else 1)
        
    except Exception as e:
        logger.error(f"Critical error: {e}")
        print(f"Critical error: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
