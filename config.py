"""
Configuration management for Snowflake DataHub Connector

This module handles configuration loading, validation, and management
for the Snowflake to DataHub metadata ingestion connector.
"""

import json
import os
from typing import Dict, List, Optional, Any
from dataclasses import dataclass, field
from pydantic import BaseModel, Field, validator, ValidationError
import logging
from dotenv import load_dotenv

logger = logging.getLogger(__name__)

# Load environment variables from .env file
load_dotenv()


class SnowflakeConfig(BaseModel):
    """Snowflake connection configuration."""
    
    account: str = Field(..., description="Snowflake account identifier")
    username: str = Field(..., description="Snowflake username")
    password: str = Field(..., description="Snowflake password")
    warehouse: Optional[str] = Field(None, description="Snowflake warehouse")
    database: Optional[str] = Field(None, description="Default database")
    schema: Optional[str] = Field(None, description="Default schema")
    role: Optional[str] = Field(None, description="Snowflake role")
    timeout: int = Field(300, description="Connection timeout in seconds")
    
    @validator('account')
    def validate_account(cls, v):
        if not v or not v.strip():
            raise ValueError("Snowflake account cannot be empty")
        return v.strip()
    
    @validator('username')
    def validate_username(cls, v):
        if not v or not v.strip():
            raise ValueError("Snowflake username cannot be empty")
        return v.strip()
    
    @validator('password')
    def validate_password(cls, v):
        if not v or not v.strip():
            raise ValueError("Snowflake password cannot be empty")
        return v
    
    @validator('timeout')
    def validate_timeout(cls, v):
        if v <= 0:
            raise ValueError("Timeout must be positive")
        return v

    class Config:
        """Pydantic configuration."""
        env_prefix = "SNOWFLAKE_"


class DataHubConfig(BaseModel):
    """DataHub connection configuration."""
    
    server_url: str = Field(..., description="DataHub server URL")
    token: Optional[str] = Field(None, description="DataHub authentication token")
    timeout: int = Field(30, description="Request timeout in seconds")
    max_retries: int = Field(3, description="Maximum number of retries")
    batch_size: int = Field(100, description="Batch size for ingestion")
    
    @validator('server_url')
    def validate_server_url(cls, v):
        if not v or not v.strip():
            raise ValueError("DataHub server URL cannot be empty")
        url = v.strip()
        if not url.startswith(('http://', 'https://')):
            raise ValueError("DataHub server URL must start with http:// or https://")
        return url.rstrip('/')
    
    @validator('timeout')
    def validate_timeout(cls, v):
        if v <= 0:
            raise ValueError("Timeout must be positive")
        return v
    
    @validator('max_retries')
    def validate_max_retries(cls, v):
        if v < 0:
            raise ValueError("Max retries cannot be negative")
        return v
    
    @validator('batch_size')
    def validate_batch_size(cls, v):
        if v <= 0:
            raise ValueError("Batch size must be positive")
        return v

    class Config:
        """Pydantic configuration."""
        env_prefix = "DATAHUB_"


class Config(BaseModel):
    """Main configuration for the connector."""
    
    snowflake: SnowflakeConfig
    datahub: DataHubConfig
    
    # Platform configuration
    platform: str = Field("snowflake", description="Data platform name (snowflake, databricks, etc.)")
    
    # Metadata extraction options
    extract_structural_metadata: bool = Field(True, description="Extract structural metadata")
    extract_access_control: bool = Field(True, description="Extract access control metadata")
    
    # Filtering options
    include_databases: Optional[List[str]] = Field(None, description="Databases to include")
    exclude_databases: Optional[List[str]] = Field(None, description="Databases to exclude")
    include_schemas: Optional[List[str]] = Field(None, description="Schemas to include")
    exclude_schemas: Optional[List[str]] = Field(None, description="Schemas to exclude")
    include_tables: Optional[List[str]] = Field(None, description="Tables to include")
    exclude_tables: Optional[List[str]] = Field(None, description="Tables to exclude")
    
    # Logging configuration
    log_level: str = Field("INFO", description="Logging level")
    log_format: str = Field(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        description="Log format"
    )
    
    @validator('log_level')
    def validate_log_level(cls, v):
        valid_levels = ['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']
        if v.upper() not in valid_levels:
            raise ValueError(f"Log level must be one of {valid_levels}")
        return v.upper()
    
    @validator('include_databases', 'exclude_databases', 'include_schemas', 'exclude_schemas', 'include_tables', 'exclude_tables')
    def validate_lists(cls, v):
        if v is not None and len(v) == 0:
            return None
        return v
    
    @classmethod
    def load_config(cls, config_file: str = "config.json") -> 'Config':
        """
        Load configuration from file and environment variables.
        
        Args:
            config_file: Path to configuration file
            
        Returns:
            Config: Validated configuration object
        """
        try:
            # Load from file if it exists
            file_config = {}
            if os.path.exists(config_file):
                logger.info(f"Loading configuration from {config_file}")
                with open(config_file, 'r') as f:
                    file_config = json.load(f)
            else:
                logger.info(f"Configuration file {config_file} not found, using environment variables")
            
            # Build configuration from environment variables and file
            config_data = cls._build_config_from_env(file_config)
            
            # Validate and return configuration
            config = cls(**config_data)
            logger.info("Configuration loaded and validated successfully")
            return config
            
        except ValidationError as e:
            logger.error(f"Configuration validation error: {e}")
            raise ValueError(f"Configuration validation failed: {e}")
        except Exception as e:
            logger.error(f"Error loading configuration: {e}")
            raise
    
    @classmethod
    def _build_config_from_env(cls, file_config: Dict[str, Any]) -> Dict[str, Any]:
        """Build configuration from environment variables and file config."""
        
        # Snowflake configuration
        snowflake_config = file_config.get('snowflake', {})
        snowflake_config.update({
            'account': os.getenv('SNOWFLAKE_ACCOUNT', snowflake_config.get('account')),
            'username': os.getenv('SNOWFLAKE_USERNAME', snowflake_config.get('username')),
            'password': os.getenv('SNOWFLAKE_PASSWORD', snowflake_config.get('password')),
            'warehouse': os.getenv('SNOWFLAKE_WAREHOUSE', snowflake_config.get('warehouse')),
            'database': os.getenv('SNOWFLAKE_DATABASE', snowflake_config.get('database')),
            'schema': os.getenv('SNOWFLAKE_SCHEMA', snowflake_config.get('schema')),
            'role': os.getenv('SNOWFLAKE_ROLE', snowflake_config.get('role')),
            'timeout': int(os.getenv('SNOWFLAKE_TIMEOUT', snowflake_config.get('timeout', 300))),
        })
        
        # DataHub configuration
        datahub_config = file_config.get('datahub', {})
        datahub_config.update({
            'server_url': os.getenv('DATAHUB_SERVER_URL', datahub_config.get('server_url')),
            'token': os.getenv('DATAHUB_TOKEN', datahub_config.get('token')),
            'timeout': int(os.getenv('DATAHUB_TIMEOUT', datahub_config.get('timeout', 30))),
            'max_retries': int(os.getenv('DATAHUB_MAX_RETRIES', datahub_config.get('max_retries', 3))),
            'batch_size': int(os.getenv('DATAHUB_BATCH_SIZE', datahub_config.get('batch_size', 100))),
        })
        
        # Main configuration
        config_data = {
            'snowflake': snowflake_config,
            'datahub': datahub_config,
            'platform': os.getenv('PLATFORM', file_config.get('platform', 'snowflake')),
            'extract_structural_metadata': cls._get_bool_env('EXTRACT_STRUCTURAL_METADATA', 
                                                            file_config.get('extract_structural_metadata', True)),
            'extract_access_control': cls._get_bool_env('EXTRACT_ACCESS_CONTROL', 
                                                       file_config.get('extract_access_control', True)),
            'include_databases': cls._get_list_env('INCLUDE_DATABASES', 
                                                  file_config.get('include_databases')),
            'exclude_databases': cls._get_list_env('EXCLUDE_DATABASES', 
                                                  file_config.get('exclude_databases')),
            'include_schemas': cls._get_list_env('INCLUDE_SCHEMAS', 
                                                file_config.get('include_schemas')),
            'exclude_schemas': cls._get_list_env('EXCLUDE_SCHEMAS', 
                                                file_config.get('exclude_schemas')),
            'include_tables': cls._get_list_env('INCLUDE_TABLES', 
                                               file_config.get('include_tables')),
            'exclude_tables': cls._get_list_env('EXCLUDE_TABLES', 
                                               file_config.get('exclude_tables')),
            'log_level': os.getenv('LOG_LEVEL', file_config.get('log_level', 'INFO')),
            'log_format': os.getenv('LOG_FORMAT', file_config.get('log_format', 
                                                                 '%(asctime)s - %(name)s - %(levelname)s - %(message)s')),
        }
        
        return config_data
    
    @staticmethod
    def _get_bool_env(key: str, default: bool) -> bool:
        """Get boolean value from environment variable."""
        value = os.getenv(key)
        if value is None:
            return default
        return value.lower() in ('true', '1', 'yes', 'on')
    
    @staticmethod
    def _get_list_env(key: str, default: Optional[List[str]]) -> Optional[List[str]]:
        """Get list value from environment variable."""
        value = os.getenv(key)
        if value is None:
            return default
        return [item.strip() for item in value.split(',') if item.strip()]
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert configuration to dictionary."""
        return self.dict()
    
    def save_config(self, config_file: str = "config.json"):
        """Save configuration to file."""
        try:
            config_dict = self.to_dict()
            
            # Remove sensitive information
            if 'password' in config_dict.get('snowflake', {}):
                config_dict['snowflake']['password'] = '***'
            if 'token' in config_dict.get('datahub', {}):
                config_dict['datahub']['token'] = '***'
            
            with open(config_file, 'w') as f:
                json.dump(config_dict, f, indent=2)
            
            logger.info(f"Configuration saved to {config_file}")
            
        except Exception as e:
            logger.error(f"Error saving configuration: {e}")
            raise
