"""
Command Line Interface for Snowflake DataHub Connector

This module provides a CLI for running the Snowflake to DataHub
metadata ingestion connector.
"""

import sys
import json
import logging
from pathlib import Path
from typing import Optional
import click
from datetime import datetime

from main import SnowflakeDataHubConnector
from config import Config
from snowflake_connector import SnowflakeConnector
from datahub_client import DataHubClient
from models import ConnectionTest
from utils import setup_logging, Timer

logger = logging.getLogger(__name__)


@click.group()
@click.option('--config', '-c', default='config.json', help='Configuration file path')
@click.option('--log-level', '-l', default='INFO', help='Logging level', 
              type=click.Choice(['DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL']))
@click.option('--verbose', '-v', is_flag=True, help='Enable verbose logging')
@click.pass_context
def cli(ctx, config, log_level, verbose):
    """Snowflake to DataHub metadata ingestion connector."""
    # Ensure that ctx.obj exists and is a dict
    ctx.ensure_object(dict)
    ctx.obj['config_file'] = config
    ctx.obj['log_level'] = 'DEBUG' if verbose else log_level
    
    # Setup logging
    setup_logging(level=ctx.obj['log_level'])


@cli.command()
@click.option('--extract-structural/--no-extract-structural', default=True,
              help='Extract structural metadata (tables, schemas, etc.)')
@click.option('--extract-access-control/--no-extract-access-control', default=True,
              help='Extract access control metadata (users, roles)')
@click.option('--dry-run', is_flag=True, help='Run without actually ingesting data')
@click.pass_context
def run(ctx, extract_structural, extract_access_control, dry_run):
    """Run the metadata ingestion process."""
    try:
        logger.info("Starting Snowflake to DataHub metadata ingestion")
        
        if dry_run:
            logger.info("DRY RUN MODE: No data will be ingested")
        
        # Load configuration
        config = Config.load_config(ctx.obj['config_file'])
        
        # Override config with CLI options
        config.extract_structural_metadata = extract_structural
        config.extract_access_control = extract_access_control
        
        # Create and run connector
        connector = SnowflakeDataHubConnector(config)
        
        with Timer("Metadata ingestion", logger):
            result = connector.extract_and_ingest_metadata()
        
        # Display results
        click.echo("\n" + "="*50)
        click.echo("INGESTION SUMMARY")
        click.echo("="*50)
        click.echo(f"Status: {result.status}")
        click.echo(f"Duration: {result.duration:.2f} seconds")
        click.echo(f"Datasets: {result.datasets_success}/{result.datasets_processed} successful")
        click.echo(f"Users: {result.users_success}/{result.users_processed} successful")
        click.echo(f"Groups: {result.groups_success}/{result.groups_processed} successful")
        
        if result.errors:
            click.echo(f"\nErrors ({len(result.errors)}):")
            for error in result.errors:
                click.echo(f"  - {error}")
        
        # Exit with appropriate code
        sys.exit(0 if result.status == "completed" else 1)
        
    except Exception as e:
        logger.error(f"Ingestion failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.pass_context
def test(ctx):
    """Test connections to Snowflake and DataHub."""
    try:
        logger.info("Testing connections")
        
        # Load configuration
        config = Config.load_config(ctx.obj['config_file'])
        
        test_result = ConnectionTest()
        
        # Test Snowflake connection
        click.echo("Testing Snowflake connection...")
        try:
            snowflake_connector = SnowflakeConnector(config.snowflake)
            snowflake_connector.connect()
            
            # Test permissions
            permissions = snowflake_connector.test_permissions()
            test_result.permissions = permissions
            
            snowflake_connector.disconnect()
            test_result.snowflake_connected = True
            click.echo("✓ Snowflake connection successful")
            
        except Exception as e:
            test_result.snowflake_connected = False
            test_result.snowflake_error = str(e)
            click.echo(f"✗ Snowflake connection failed: {e}")
        
        # Test DataHub connection
        click.echo("Testing DataHub connection...")
        try:
            datahub_client = DataHubClient(config.datahub, platform=config.platform)
            health = datahub_client.health_check()
            
            if health.get('status') == 'healthy' or datahub_client.test_connection():
                test_result.datahub_connected = True
                click.echo("✓ DataHub connection successful")
            else:
                test_result.datahub_connected = False
                test_result.datahub_error = f"Health check failed: {health}"
                click.echo(f"✗ DataHub connection failed: {health}")
                
        except Exception as e:
            test_result.datahub_connected = False
            test_result.datahub_error = str(e)
            click.echo(f"✗ DataHub connection failed: {e}")
        
        # Display results
        click.echo("\n" + "="*50)
        click.echo("CONNECTION TEST SUMMARY")
        click.echo("="*50)
        
        if test_result.is_ready():
            click.echo("✓ All connections successful - ready to run ingestion")
        else:
            click.echo("✗ Some connections failed:")
            for issue in test_result.get_issues():
                click.echo(f"  - {issue}")
        
        # Display permissions
        if test_result.permissions:
            click.echo("\nSnowflake Permissions:")
            for perm, granted in test_result.permissions.items():
                status = "✓" if granted else "✗"
                click.echo(f"  {status} {perm}")
        
        sys.exit(0 if test_result.is_ready() else 1)
        
    except Exception as e:
        logger.error(f"Connection test failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', help='Output file path (default: stdout)')
@click.option('--format', 'output_format', default='json', 
              type=click.Choice(['json', 'yaml']), help='Output format')
@click.pass_context
def config_template(ctx, output, output_format):
    """Generate a configuration template."""
    try:
        template = {
            "snowflake": {
                "account": "your-account.snowflakecomputing.com",
                "username": "your-username",
                "password": "your-password",
                "warehouse": "your-warehouse",
                "database": "your-database",
                "schema": "your-schema",
                "role": "your-role",
                "timeout": 300
            },
            "datahub": {
                "server_url": "http://localhost:8080",
                "token": "your-datahub-token",
                "timeout": 30,
                "max_retries": 3,
                "batch_size": 100
            },
            "platform": "snowflake",
            "extract_structural_metadata": True,
            "extract_access_control": True,
            "include_databases": None,
            "exclude_databases": ["INFORMATION_SCHEMA", "PERFORMANCE_SCHEMA"],
            "include_schemas": None,
            "exclude_schemas": ["INFORMATION_SCHEMA"],
            "include_tables": None,
            "exclude_tables": None,
            "log_level": "INFO",
            "log_format": "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
        }
        
        if output_format == 'json':
            content = json.dumps(template, indent=2)
        else:
            # Simple YAML-like format
            content = "# Snowflake DataHub Connector Configuration\n"
            content += "# Copy this template and fill in your values\n\n"
            content += json.dumps(template, indent=2)
        
        if output:
            with open(output, 'w') as f:
                f.write(content)
            click.echo(f"Configuration template saved to: {output}")
        else:
            click.echo(content)
            
    except Exception as e:
        logger.error(f"Failed to generate config template: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--database', '-d', help='Specific database to explore')
@click.option('--schema', '-s', help='Specific schema to explore')
@click.option('--limit', '-l', default=10, help='Limit number of results')
@click.pass_context
def explore(ctx, database, schema, limit):
    """Explore Snowflake metadata without ingesting."""
    try:
        logger.info("Exploring Snowflake metadata")
        
        # Load configuration
        config = Config.load_config(ctx.obj['config_file'])
        
        # Connect to Snowflake
        snowflake_connector = SnowflakeConnector(config.snowflake)
        snowflake_connector.connect()
        
        try:
            if database and schema:
                # Explore specific schema
                click.echo(f"Exploring {database}.{schema}:")
                tables = snowflake_connector.get_tables(database, schema)
                
                for i, table in enumerate(tables[:limit]):
                    click.echo(f"  {i+1}. {table['TABLE_NAME']} ({table['TABLE_TYPE']})")
                    if table.get('ROW_COUNT'):
                        click.echo(f"     Rows: {table['ROW_COUNT']}")
                    if table.get('COMMENT'):
                        click.echo(f"     Description: {table['COMMENT']}")
                
            elif database:
                # Explore specific database
                click.echo(f"Exploring database: {database}")
                schemas = snowflake_connector.get_schemas(database)
                
                for i, schema_name in enumerate(schemas[:limit]):
                    click.echo(f"  {i+1}. {schema_name}")
                    
            else:
                # Explore all databases
                click.echo("Exploring all databases:")
                databases = snowflake_connector.get_databases()
                
                for i, db_name in enumerate(databases[:limit]):
                    click.echo(f"  {i+1}. {db_name}")
                    
                    # Show a few schemas for each database
                    try:
                        schemas = snowflake_connector.get_schemas(db_name)
                        for j, schema_name in enumerate(schemas[:3]):
                            click.echo(f"     - {schema_name}")
                        if len(schemas) > 3:
                            click.echo(f"     ... and {len(schemas)-3} more schemas")
                    except Exception as e:
                        click.echo(f"     Error accessing schemas: {e}")
        
        finally:
            snowflake_connector.disconnect()
            
    except Exception as e:
        logger.error(f"Exploration failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


@cli.command()
@click.option('--output', '-o', help='Output file path (default: stdout)')
@click.pass_context
def validate_config(ctx, output):
    """Validate configuration file."""
    try:
        logger.info(f"Validating configuration: {ctx.obj['config_file']}")
        
        # Try to load and validate config
        config = Config.load_config(ctx.obj['config_file'])
        
        validation_result = {
            'status': 'valid',
            'file': ctx.obj['config_file'],
            'timestamp': datetime.now().isoformat(),
            'issues': []
        }
        
        # Additional validation checks
        if not config.snowflake.account:
            validation_result['issues'].append("Snowflake account is required")
        
        if not config.datahub.server_url:
            validation_result['issues'].append("DataHub server URL is required")
        
        if config.datahub.batch_size <= 0:
            validation_result['issues'].append("Batch size must be positive")
        
        if validation_result['issues']:
            validation_result['status'] = 'invalid'
        
        # Output results
        result_json = json.dumps(validation_result, indent=2)
        
        if output:
            with open(output, 'w') as f:
                f.write(result_json)
            click.echo(f"Validation results saved to: {output}")
        else:
            click.echo(result_json)
        
        if validation_result['status'] == 'valid':
            click.echo("✓ Configuration is valid")
        else:
            click.echo("✗ Configuration has issues:")
            for issue in validation_result['issues']:
                click.echo(f"  - {issue}")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"Config validation failed: {e}")
        click.echo(f"Error: {e}", err=True)
        sys.exit(1)


if __name__ == '__main__':
    cli()
