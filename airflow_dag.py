"""
Airflow DAG for Snowflake to DataHub metadata ingestion

This DAG runs the Snowflake DataHub connector as an Airflow task
using the edge executor for distributed execution.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago
import os
import logging
from pathlib import Path

# Import our connector modules
from main import SnowflakeDataHubConnector
from config import Config
from models import IngestionResult

# Default DAG arguments
default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

# Create DAG
dag = DAG(
    'snowflake_datahub_metadata_ingestion',
    default_args=default_args,
    description='Extract metadata from Snowflake and ingest into DataHub',
    schedule_interval='0 2 * * *',  # Daily at 2 AM
    catchup=False,
    max_active_runs=1,
    tags=['metadata', 'snowflake', 'datahub', 'data-catalog'],
)

def validate_environment():
    """Validate that all required environment variables are set"""
    required_vars = [
        'SNOWFLAKE_ACCOUNT',
        'SNOWFLAKE_USERNAME', 
        'SNOWFLAKE_PASSWORD',
        'DATAHUB_SERVER_URL'
    ]
    
    missing_vars = []
    for var in required_vars:
        if not os.getenv(var):
            missing_vars.append(var)
    
    if missing_vars:
        raise ValueError(f"Missing required environment variables: {', '.join(missing_vars)}")
    
    logging.info("Environment validation passed")
    return True

def test_connections():
    """Test connections to both Snowflake and DataHub before running ingestion"""
    try:
        # Load configuration
        config = Config.load_config()
        
        # Initialize connector
        connector = SnowflakeDataHubConnector(config)
        
        # Test Snowflake connection
        connector.snowflake_connector.connect()
        connector.snowflake_connector.execute_query("SELECT 1")
        logging.info("Snowflake connection test passed")
        
        # Test DataHub connection
        health_check = connector.datahub_client.health_check()
        if not health_check.get('status') == 'ok':
            raise Exception("DataHub health check failed")
        logging.info("DataHub connection test passed")
        
        # Clean up
        connector.snowflake_connector.disconnect()
        
        return True
        
    except Exception as e:
        logging.error(f"Connection test failed: {str(e)}")
        raise

def run_metadata_ingestion():
    """Main function to run the metadata ingestion process"""
    try:
        # Load configuration
        config = Config.load_config()
        
        # Initialize connector
        connector = SnowflakeDataHubConnector(config)
        
        # Run ingestion
        logging.info("Starting metadata ingestion process")
        result = connector.extract_and_ingest_metadata()
        
        # Log results
        logging.info(f"Ingestion completed: {result.get_summary()}")
        
        # Check if ingestion was successful
        if result.status == 'failed':
            raise Exception(f"Ingestion failed with errors: {result.errors}")
        
        # Store results in XCom for downstream tasks
        return {
            'status': result.status,
            'duration': result.duration,
            'datasets_processed': result.datasets_processed,
            'datasets_success': result.datasets_success,
            'users_processed': result.users_processed,
            'users_success': result.users_success,
            'groups_processed': result.groups_processed,
            'groups_success': result.groups_success,
            'summary': result.get_summary()
        }
        
    except Exception as e:
        logging.error(f"Metadata ingestion failed: {str(e)}")
        raise

def send_completion_notification(**context):
    """Send notification about ingestion completion"""
    # Get results from previous task
    task_instance = context['task_instance']
    results = task_instance.xcom_pull(task_ids='run_ingestion')
    
    # Log completion
    logging.info(f"Metadata ingestion completed successfully")
    logging.info(f"Results: {results['summary']}")
    
    # Here you could add email notifications, Slack messages, etc.
    # For example:
    # send_slack_notification(results)
    # send_email_notification(results)
    
    return results

# Define tasks
validate_env_task = PythonOperator(
    task_id='validate_environment',
    python_callable=validate_environment,
    dag=dag,
)

test_connections_task = PythonOperator(
    task_id='test_connections',
    python_callable=test_connections,
    dag=dag,
)

run_ingestion_task = PythonOperator(
    task_id='run_ingestion',
    python_callable=run_metadata_ingestion,
    dag=dag,
)

completion_task = PythonOperator(
    task_id='send_completion_notification',
    python_callable=send_completion_notification,
    dag=dag,
)

# Alternative: Use BashOperator to run CLI directly
run_cli_task = BashOperator(
    task_id='run_cli_alternative',
    bash_command='cd /opt/airflow/dags/snowflake_datahub_connector && python cli.py run --config config.json',
    dag=dag,
)

# Set task dependencies
validate_env_task >> test_connections_task >> run_ingestion_task >> completion_task

# Optional: Add the CLI alternative as a separate branch
# validate_env_task >> test_connections_task >> run_cli_task