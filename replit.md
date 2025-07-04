# Snowflake to DataHub Metadata Connector

## Overview

This is a Python-based metadata connector that extracts metadata from Snowflake databases and ingests it into DataHub for comprehensive data catalog management. The application provides a command-line interface for extracting both structural metadata (tables, views, schemas, columns) and access control metadata (users, roles, permissions) from Snowflake and publishing it to DataHub via REST API.

## System Architecture

The connector follows a modular architecture with clear separation of concerns:

1. **CLI Layer**: Command-line interface for user interaction and configuration
2. **Core Logic**: Main connector orchestrating the extraction and ingestion process
3. **Data Sources**: Snowflake connector for metadata extraction
4. **Data Destination**: DataHub client for metadata ingestion
5. **Configuration Management**: File-based and environment variable configuration
6. **Data Models**: Pydantic models for metadata representation

## Key Components

### Main Connector (`main.py`)
- **Purpose**: Orchestrates the entire metadata extraction and ingestion process
- **Key Features**: 
  - Manages the flow between Snowflake extraction and DataHub ingestion
  - Handles error recovery and retry logic
  - Provides comprehensive logging and monitoring

### Snowflake Connector (`snowflake_connector.py`)
- **Purpose**: Handles connection to Snowflake and metadata extraction
- **Key Features**:
  - Secure connection management with timeout configuration
  - Extraction of structural metadata (databases, schemas, tables, columns)
  - Extraction of access control metadata (users, roles, permissions)
  - Context managers for proper resource cleanup

### DataHub Client (`datahub_client.py`)
- **Purpose**: Manages communication with DataHub REST API
- **Key Features**:
  - HTTP session management with retry strategies
  - Batch processing for efficient ingestion
  - Authentication token management
  - Comprehensive error handling for API interactions

### Configuration Management (`config.py`)
- **Purpose**: Handles configuration loading and validation
- **Key Features**:
  - Support for both file-based (JSON) and environment variable configuration
  - Pydantic-based validation with detailed error messages
  - Sensitive data handling (passwords, tokens)
  - Flexible filtering options for databases, schemas, and tables

### Command Line Interface (`cli.py`)
- **Purpose**: Provides user-friendly CLI for connector operations
- **Key Features**:
  - Click-based CLI with intuitive commands
  - Configuration file specification
  - Logging level control
  - Dry-run capability for testing

### Data Models (`models.py`)
- **Purpose**: Defines data structures for metadata representation
- **Key Components**:
  - `DatasetMetadata`: Represents tables and views
  - `SchemaMetadata`: Represents database schemas
  - `FieldMetadata`: Represents columns and their properties
  - `UserMetadata` and `GroupMetadata`: Represent access control entities
  - `IngestionResult`: Tracks ingestion statistics and results

### Utilities (`utils.py`)
- **Purpose**: Provides common utility functions
- **Key Features**:
  - Logging configuration with appropriate levels
  - Retry mechanisms with exponential backoff
  - Timer utilities for performance monitoring

## Data Flow

1. **Configuration Loading**: Load configuration from JSON file or environment variables
2. **Connection Establishment**: Connect to both Snowflake and DataHub
3. **Metadata Extraction**: 
   - Extract structural metadata (databases, schemas, tables, columns)
   - Extract access control metadata (users, roles, permissions)
4. **Data Transformation**: Convert Snowflake metadata to DataHub-compatible format
5. **Batch Ingestion**: Send metadata to DataHub in configurable batches
6. **Result Reporting**: Provide comprehensive ingestion statistics and error reporting

## External Dependencies

### Core Dependencies
- **snowflake-connector-python**: Official Snowflake Python connector
- **requests**: HTTP library for DataHub API communication
- **pydantic**: Data validation and settings management
- **click**: Command-line interface framework
- **python-dotenv**: Environment variable management

### Infrastructure Dependencies
- **Snowflake Database**: Source system for metadata extraction
- **DataHub Instance**: Target system for metadata ingestion with REST API enabled

## Deployment Strategy

The application is designed as a standalone Python application with the following deployment options:

1. **Direct Execution**: Run directly with Python interpreter
2. **Containerization**: Package as Docker container for consistent deployment
3. **Scheduled Execution**: Run as cron job or scheduled task for regular metadata sync
4. **CI/CD Integration**: Integrate into data pipelines for automated metadata management

### Configuration Management
- Primary configuration via `config.json` file
- Environment variable override capability
- Sensitive data handling through environment variables
- Flexible filtering options for selective metadata extraction

### Error Handling and Monitoring
- Comprehensive error handling with retry mechanisms
- Detailed logging at multiple levels
- Connection testing and validation
- Graceful degradation and recovery strategies

## Changelog
- July 03, 2025: Updated to use custom DataHub entity types `PlatformUser` and `PlatformUserGroup` instead of standard `corpuser` and `corpGroup` for better multi-platform support
- July 03, 2025: Initial setup

## User Preferences

Preferred communication style: Simple, everyday language.