# Airflow 3 Edge Executor Deployment Guide

## Overview

Yes, this Snowflake to DataHub connector can run as an Airflow 3 edge executor task. The connector is designed to be deployment-agnostic and works well in distributed environments.

## Deployment Options

### Option 1: Python Operator (Recommended)
Use the provided `airflow_dag.py` which imports the connector modules directly:

```python
from main import SnowflakeDataHubConnector
from config import Config

def run_metadata_ingestion():
    config = Config.load_config()
    connector = SnowflakeDataHubConnector(config)
    return connector.extract_and_ingest_metadata()
```

### Option 2: Bash Operator with CLI
Use the CLI interface via BashOperator:

```python
BashOperator(
    task_id='run_snowflake_datahub',
    bash_command='python cli.py run --config config.json',
    dag=dag,
)
```

### Option 3: Docker Container
Deploy as a containerized task using the provided Dockerfile:

```python
from airflow.operators.docker_operator import DockerOperator

DockerOperator(
    task_id='run_snowflake_datahub',
    image='snowflake-datahub-connector:latest',
    command='python cli.py run --config config.json',
    dag=dag,
)
```

## Airflow 3 Edge Executor Benefits

### 1. **Distributed Execution**
- Edge executor runs tasks on remote workers
- Connector can run on dedicated metadata processing nodes
- Scales horizontally with metadata volume

### 2. **Resource Isolation**
- Each task runs in isolated environment
- No resource contention with other workflows
- Dedicated CPU/memory for metadata processing

### 3. **Fault Tolerance**
- Automatic retry on task failures
- Task state persistence
- Graceful handling of worker failures

### 4. **Scheduling Flexibility**
- Cron-based scheduling (e.g., daily at 2 AM)
- Dependency management with other data pipelines
- Conditional execution based on upstream tasks

## Configuration for Airflow

### Environment Variables
Set these in your Airflow environment:

```bash
# Snowflake Connection
export SNOWFLAKE_ACCOUNT="your-account"
export SNOWFLAKE_USERNAME="your-username"
export SNOWFLAKE_PASSWORD="your-password"
export SNOWFLAKE_WAREHOUSE="your-warehouse"
export SNOWFLAKE_DATABASE="your-database"
export SNOWFLAKE_ROLE="your-role"

# DataHub Connection
export DATAHUB_SERVER_URL="http://your-datahub:8080"
export DATAHUB_TOKEN="your-token"

# Optional: Platform configuration
export PLATFORM="snowflake"
```

### Airflow Connections (Alternative)
Create Airflow connections for credential management:

```python
from airflow.hooks.base import BaseHook

def get_snowflake_connection():
    conn = BaseHook.get_connection('snowflake_default')
    return {
        'account': conn.host,
        'username': conn.login,
        'password': conn.password,
        'warehouse': conn.extra_dejson.get('warehouse'),
        'database': conn.schema,
        'role': conn.extra_dejson.get('role')
    }
```

## DAG Configuration

### Sample DAG Setup
```python
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 1),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

dag = DAG(
    'snowflake_datahub_metadata',
    default_args=default_args,
    description='Extract metadata from Snowflake to DataHub',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['metadata', 'snowflake', 'datahub'],
)
```

### Task Dependencies
```python
validate_env >> test_connections >> run_ingestion >> send_notification
```

## Edge Executor Specific Configuration

### Worker Requirements
- Python 3.11+
- Required packages: snowflake-connector-python, requests, pydantic
- Network access to both Snowflake and DataHub
- Sufficient memory for metadata processing (2GB+ recommended)

### Performance Optimization
```python
# Configure task for edge execution
run_ingestion_task = PythonOperator(
    task_id='run_ingestion',
    python_callable=run_metadata_ingestion,
    pool='metadata_pool',  # Dedicated resource pool
    priority_weight=10,    # High priority
    queue='metadata_queue', # Dedicated queue
    dag=dag,
)
```

### Monitoring and Logging
```python
# Enhanced logging for distributed execution
import logging
from airflow.utils.log.logging_mixin import LoggingMixin

class MetadataIngestionTask(LoggingMixin):
    def execute(self, context):
        self.log.info("Starting metadata ingestion on edge executor")
        # ... connector execution
        self.log.info("Metadata ingestion completed successfully")
```

## Deployment Steps

### 1. Package the Connector
```bash
# Create deployment package
tar -czf snowflake-datahub-connector.tar.gz *.py config.json

# Or use Docker
docker build -t snowflake-datahub-connector:latest .
```

### 2. Deploy to Airflow
```bash
# Copy DAG file
cp airflow_dag.py $AIRFLOW_HOME/dags/

# Copy connector modules
cp -r connector_modules/ $AIRFLOW_HOME/dags/

# Set environment variables
export AIRFLOW__CORE__EXECUTOR=EdgeExecutor
```

### 3. Configure Edge Workers
```yaml
# airflow.cfg
[edge]
worker_class = airflow.executors.edge_executor.EdgeWorker
worker_concurrency = 4
worker_log_server_port = 8793
```

### 4. Test Deployment
```bash
# Test the DAG
airflow dags test snowflake_datahub_metadata_ingestion

# Check task status
airflow tasks test snowflake_datahub_metadata_ingestion run_ingestion
```

## Best Practices

### 1. **Resource Management**
- Use dedicated worker pools for metadata tasks
- Configure appropriate timeouts (2+ hours for large datasets)
- Monitor memory usage during execution

### 2. **Error Handling**
- Implement comprehensive retry logic
- Set up alerts for task failures
- Log detailed error information

### 3. **Security**
- Use Airflow connections for credential management
- Encrypt sensitive configuration values
- Implement proper access controls

### 4. **Monitoring**
- Set up DataHub ingestion monitoring
- Track task execution metrics
- Monitor Snowflake connection health

## Troubleshooting

### Common Issues
1. **Connection Timeouts**: Increase timeout values in configuration
2. **Memory Issues**: Reduce batch sizes or increase worker memory
3. **Network Issues**: Ensure worker nodes can reach both Snowflake and DataHub
4. **Permission Issues**: Verify Snowflake and DataHub credentials

### Debugging
```python
# Enable debug logging
logging.basicConfig(level=logging.DEBUG)

# Add task debugging
def debug_task(**context):
    print(f"Task execution context: {context}")
    print(f"Environment variables: {os.environ}")
```

The connector is fully compatible with Airflow 3 edge executor and provides robust, scalable metadata ingestion capabilities in distributed environments.