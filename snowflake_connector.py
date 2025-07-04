"""
Snowflake Connector for metadata extraction

This module provides functionality to connect to Snowflake and extract
various types of metadata including structural and access control information.
"""

import logging
from typing import Dict, List, Optional, Any
from contextlib import contextmanager
import snowflake.connector
from snowflake.connector import DictCursor
from snowflake.connector.errors import (
    ProgrammingError, DatabaseError, InterfaceError, 
    OperationalError, InternalError
)

from config import SnowflakeConfig

logger = logging.getLogger(__name__)


class SnowflakeConnector:
    """Connector for Snowflake metadata extraction."""
    
    def __init__(self, config: SnowflakeConfig):
        """Initialize the Snowflake connector."""
        self.config = config
        self.connection = None
        self._connected = False
    
    def connect(self) -> None:
        """Establish connection to Snowflake."""
        try:
            logger.info(f"Connecting to Snowflake account: {self.config.account}")
            
            connection_params = {
                'account': self.config.account,
                'user': self.config.username,
                'password': self.config.password,
                'client_session_keep_alive': True,
                'network_timeout': self.config.timeout,
                'login_timeout': self.config.timeout,
            }
            
            # Add optional parameters
            if self.config.warehouse:
                connection_params['warehouse'] = self.config.warehouse
            if self.config.database:
                connection_params['database'] = self.config.database
            if self.config.schema:
                connection_params['schema'] = self.config.schema
            if self.config.role:
                connection_params['role'] = self.config.role
            
            self.connection = snowflake.connector.connect(**connection_params)
            self._connected = True
            
            logger.info("Successfully connected to Snowflake")
            
            # Test the connection
            self._test_connection()
            
        except (ProgrammingError, DatabaseError, InterfaceError, OperationalError) as e:
            logger.error(f"Failed to connect to Snowflake: {e}")
            raise ConnectionError(f"Snowflake connection failed: {e}")
        except Exception as e:
            logger.error(f"Unexpected error connecting to Snowflake: {e}")
            raise
    
    def disconnect(self) -> None:
        """Close connection to Snowflake."""
        if self.connection and self._connected:
            try:
                self.connection.close()
                logger.info("Disconnected from Snowflake")
            except Exception as e:
                logger.warning(f"Error disconnecting from Snowflake: {e}")
            finally:
                self._connected = False
                self.connection = None
    
    def _test_connection(self) -> None:
        """Test the Snowflake connection."""
        try:
            with self._get_cursor() as cursor:
                cursor.execute("SELECT CURRENT_VERSION()")
                result = cursor.fetchone()
                logger.info(f"Snowflake version: {result[0]}")
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            raise
    
    @contextmanager
    def _get_cursor(self):
        """Get a cursor for executing queries."""
        if not self._connected or not self.connection:
            raise ConnectionError("Not connected to Snowflake")
        
        cursor = self.connection.cursor(DictCursor)
        try:
            yield cursor
        finally:
            cursor.close()
    
    def execute_query(self, query: str, params: Optional[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
        """
        Execute a query and return results.
        
        Args:
            query: SQL query to execute
            params: Query parameters
            
        Returns:
            List of dictionaries containing query results
        """
        try:
            with self._get_cursor() as cursor:
                if params:
                    cursor.execute(query, params)
                else:
                    cursor.execute(query)
                
                results = cursor.fetchall()
                return results
                
        except ProgrammingError as e:
            logger.error(f"SQL execution error: {e}")
            raise
        except Exception as e:
            logger.error(f"Unexpected error executing query: {e}")
            raise
    
    def get_databases(self) -> List[str]:
        """Get list of databases."""
        try:
            logger.debug("Fetching databases")
            query = "SHOW DATABASES"
            results = self.execute_query(query)
            
            databases = [row['name'] for row in results]
            logger.info(f"Found {len(databases)} databases")
            return databases
            
        except Exception as e:
            logger.error(f"Error fetching databases: {e}")
            raise
    
    def get_schemas(self, database: str) -> List[str]:
        """Get list of schemas in a database."""
        try:
            logger.debug(f"Fetching schemas for database: {database}")
            query = f"SHOW SCHEMAS IN DATABASE {database}"
            results = self.execute_query(query)
            
            schemas = [row['name'] for row in results]
            logger.debug(f"Found {len(schemas)} schemas in {database}")
            return schemas
            
        except Exception as e:
            logger.error(f"Error fetching schemas for database {database}: {e}")
            raise
    
    def get_tables(self, database: str, schema: str) -> List[Dict[str, Any]]:
        """Get list of tables and views in a schema."""
        try:
            logger.debug(f"Fetching tables for {database}.{schema}")
            
            # Query for tables and views
            query = f"""
            SELECT 
                TABLE_NAME,
                TABLE_TYPE,
                ROW_COUNT,
                BYTES,
                CREATED,
                LAST_ALTERED,
                COMMENT,
                TABLE_OWNER
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}'
            ORDER BY TABLE_NAME
            """
            
            results = self.execute_query(query)
            logger.debug(f"Found {len(results)} tables in {database}.{schema}")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching tables for {database}.{schema}: {e}")
            raise
    
    def get_table_columns(self, database: str, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get column information for a table."""
        try:
            logger.debug(f"Fetching columns for {database}.{schema}.{table}")
            
            query = f"""
            SELECT 
                COLUMN_NAME,
                DATA_TYPE,
                IS_NULLABLE,
                COLUMN_DEFAULT,
                ORDINAL_POSITION,
                COMMENT
            FROM {database}.INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            ORDER BY ORDINAL_POSITION
            """
            
            results = self.execute_query(query)
            logger.debug(f"Found {len(results)} columns for {database}.{schema}.{table}")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching columns for {database}.{schema}.{table}: {e}")
            raise
    
    def get_primary_keys(self, database: str, schema: str, table: str) -> List[str]:
        """Get primary key columns for a table."""
        try:
            logger.debug(f"Fetching primary keys for {database}.{schema}.{table}")
            
            query = f"""
            SELECT COLUMN_NAME
            FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = '{schema}'
                AND tc.TABLE_NAME = '{table}'
                AND tc.CONSTRAINT_TYPE = 'PRIMARY KEY'
            ORDER BY kcu.ORDINAL_POSITION
            """
            
            results = self.execute_query(query)
            primary_keys = [row['COLUMN_NAME'] for row in results]
            logger.debug(f"Found {len(primary_keys)} primary key columns for {database}.{schema}.{table}")
            return primary_keys
            
        except Exception as e:
            logger.debug(f"Could not fetch primary keys for {database}.{schema}.{table}: {e}")
            return []
    
    def get_foreign_keys(self, database: str, schema: str, table: str) -> List[Dict[str, Any]]:
        """Get foreign key constraints for a table."""
        try:
            logger.debug(f"Fetching foreign keys for {database}.{schema}.{table}")
            
            query = f"""
            SELECT 
                kcu.COLUMN_NAME,
                kcu.REFERENCED_TABLE_SCHEMA,
                kcu.REFERENCED_TABLE_NAME,
                kcu.REFERENCED_COLUMN_NAME,
                tc.CONSTRAINT_NAME
            FROM {database}.INFORMATION_SCHEMA.TABLE_CONSTRAINTS tc
            JOIN {database}.INFORMATION_SCHEMA.KEY_COLUMN_USAGE kcu
                ON tc.CONSTRAINT_NAME = kcu.CONSTRAINT_NAME
                AND tc.TABLE_SCHEMA = kcu.TABLE_SCHEMA
                AND tc.TABLE_NAME = kcu.TABLE_NAME
            WHERE tc.TABLE_SCHEMA = '{schema}'
                AND tc.TABLE_NAME = '{table}'
                AND tc.CONSTRAINT_TYPE = 'FOREIGN KEY'
            ORDER BY kcu.ORDINAL_POSITION
            """
            
            results = self.execute_query(query)
            logger.debug(f"Found {len(results)} foreign key constraints for {database}.{schema}.{table}")
            return results
            
        except Exception as e:
            logger.debug(f"Could not fetch foreign keys for {database}.{schema}.{table}: {e}")
            return []
    
    def get_users(self) -> List[Dict[str, Any]]:
        """Get user information from Snowflake."""
        try:
            logger.debug("Fetching users")
            
            # Query users from ACCOUNT_USAGE schema
            query = """
            SELECT 
                NAME,
                EMAIL,
                DISPLAY_NAME,
                DISABLED,
                CREATED_ON,
                LAST_SUCCESS_LOGIN,
                COMMENT
            FROM SNOWFLAKE.ACCOUNT_USAGE.USERS
            WHERE DELETED_ON IS NULL
            ORDER BY NAME
            """
            
            results = self.execute_query(query)
            
            # Get role grants for each user
            for user in results:
                user['ROLES'] = self._get_user_roles(user['NAME'])
            
            logger.info(f"Found {len(results)} users")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching users: {e}")
            # Try alternative query without ACCOUNT_USAGE
            try:
                query = "SHOW USERS"
                results = self.execute_query(query)
                logger.info(f"Found {len(results)} users (from SHOW USERS)")
                return results
            except Exception as e2:
                logger.error(f"Error with alternative user query: {e2}")
                raise
    
    def get_roles(self) -> List[Dict[str, Any]]:
        """Get role information from Snowflake."""
        try:
            logger.debug("Fetching roles")
            
            # Query roles from ACCOUNT_USAGE schema
            query = """
            SELECT 
                NAME,
                COMMENT,
                CREATED_ON,
                OWNER
            FROM SNOWFLAKE.ACCOUNT_USAGE.ROLES
            WHERE DELETED_ON IS NULL
            ORDER BY NAME
            """
            
            results = self.execute_query(query)
            
            # Get role members for each role
            for role in results:
                role['MEMBERS'] = self._get_role_members(role['NAME'])
            
            logger.info(f"Found {len(results)} roles")
            return results
            
        except Exception as e:
            logger.error(f"Error fetching roles: {e}")
            # Try alternative query without ACCOUNT_USAGE
            try:
                query = "SHOW ROLES"
                results = self.execute_query(query)
                logger.info(f"Found {len(results)} roles (from SHOW ROLES)")
                return results
            except Exception as e2:
                logger.error(f"Error with alternative role query: {e2}")
                raise
    
    def _get_user_roles(self, username: str) -> List[str]:
        """Get roles granted to a user."""
        try:
            query = """
            SELECT ROLE
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
            WHERE GRANTEE_NAME = %s
                AND DELETED_ON IS NULL
            """
            
            results = self.execute_query(query, {'GRANTEE_NAME': username})
            return [row['ROLE'] for row in results]
            
        except Exception as e:
            logger.debug(f"Could not fetch roles for user {username}: {e}")
            return []
    
    def _get_role_members(self, role_name: str) -> List[str]:
        """Get members of a role."""
        try:
            query = """
            SELECT GRANTEE_NAME
            FROM SNOWFLAKE.ACCOUNT_USAGE.GRANTS_TO_USERS
            WHERE ROLE = %s
                AND DELETED_ON IS NULL
            """
            
            results = self.execute_query(query, {'ROLE': role_name})
            return [row['GRANTEE_NAME'] for row in results]
            
        except Exception as e:
            logger.debug(f"Could not fetch members for role {role_name}: {e}")
            return []
    
    def get_table_statistics(self, database: str, schema: str, table: str) -> Dict[str, Any]:
        """Get table statistics."""
        try:
            logger.debug(f"Fetching statistics for {database}.{schema}.{table}")
            
            query = f"""
            SELECT 
                ROW_COUNT,
                BYTES,
                CREATED,
                LAST_ALTERED,
                LAST_DDL,
                COMMENT
            FROM {database}.INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{schema}' AND TABLE_NAME = '{table}'
            """
            
            results = self.execute_query(query)
            if results:
                return results[0]
            return {}
            
        except Exception as e:
            logger.debug(f"Could not fetch statistics for {database}.{schema}.{table}: {e}")
            return {}
    
    def test_permissions(self) -> Dict[str, bool]:
        """Test various permissions to determine what metadata can be accessed."""
        permissions = {
            'can_access_information_schema': False,
            'can_access_account_usage': False,
            'can_show_databases': False,
            'can_show_users': False,
            'can_show_roles': False,
        }
        
        # Test INFORMATION_SCHEMA access
        try:
            self.execute_query("SELECT 1 FROM INFORMATION_SCHEMA.TABLES LIMIT 1")
            permissions['can_access_information_schema'] = True
        except Exception:
            pass
        
        # Test ACCOUNT_USAGE access
        try:
            self.execute_query("SELECT 1 FROM SNOWFLAKE.ACCOUNT_USAGE.USERS LIMIT 1")
            permissions['can_access_account_usage'] = True
        except Exception:
            pass
        
        # Test SHOW commands
        try:
            self.execute_query("SHOW DATABASES")
            permissions['can_show_databases'] = True
        except Exception:
            pass
        
        try:
            self.execute_query("SHOW USERS")
            permissions['can_show_users'] = True
        except Exception:
            pass
        
        try:
            self.execute_query("SHOW ROLES")
            permissions['can_show_roles'] = True
        except Exception:
            pass
        
        logger.info(f"Permission test results: {permissions}")
        return permissions
