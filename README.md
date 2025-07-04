# Snowflake to DataHub Metadata Connector

A Python connector that extracts metadata from Snowflake databases and ingests it into DataHub for comprehensive data catalog management.

## Features

- **Structural Metadata Extraction**: Tables, views, schemas, columns, data types, and constraints
- **Access Control Metadata**: Users, roles, and permissions
- **DataHub Integration**: REST API-based ingestion with batch processing
- **Flexible Configuration**: File-based and environment variable configuration
- **Error Handling**: Comprehensive error handling with retry mechanisms
- **CLI Interface**: Command-line interface for easy operation
- **Logging**: Detailed logging for monitoring and troubleshooting

## Prerequisites

- Python 3.8+
- Access to Snowflake database with appropriate permissions
- DataHub instance with REST API access
- Required Python packages (see requirements section)

## Installation

1. Clone or download the connector files
2. Install required Python packages:
   ```bash
   pip install snowflake-connector-python requests pydantic click python-dotenv
   ```

## Configuration

### Method 1: Configuration File

Create a `config.json` file:

```json
{
  "snowflake": {
    "account": "your-account.snowflakecomputing.com",
    "username": "your-username",
    "password": "your-password",
    "warehouse": "COMPUTE_WH",
    "database": null,
    "schema": null,
    "role": null,
    "timeout": 300
  },
  "datahub": {
    "server_url": "http://localhost:8080",
    "token": null,
    "timeout": 30,
    "max_retries": 3,
    "batch_size": 100
  },
  "extract_structural_metadata": true,
  "extract_access_control": true,
  "include_databases": null,
  "exclude_databases": ["INFORMATION_SCHEMA"],
  "log_level": "INFO"
}
