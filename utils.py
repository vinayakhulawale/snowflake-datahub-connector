"""
Utility functions for Snowflake DataHub Connector

This module provides utility functions for logging, retry mechanisms,
and other common functionality.
"""

import logging
import time
import functools
from typing import Callable, Any, Optional
from datetime import datetime
import sys
import os


def setup_logging(level: str = "INFO", format_str: Optional[str] = None) -> None:
    """
    Set up logging configuration.
    
    Args:
        level: Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        format_str: Custom log format string
    """
    if format_str is None:
        format_str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    
    # Convert string level to logging level
    numeric_level = getattr(logging, level.upper(), None)
    if not isinstance(numeric_level, int):
        raise ValueError(f'Invalid log level: {level}')
    
    # Configure logging
    logging.basicConfig(
        level=numeric_level,
        format=format_str,
        datefmt='%Y-%m-%d %H:%M:%S',
        stream=sys.stdout
    )
    
    # Set third-party loggers to higher levels to reduce noise
    logging.getLogger('snowflake.connector').setLevel(logging.WARNING)
    logging.getLogger('requests').setLevel(logging.WARNING)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    
    logger = logging.getLogger(__name__)
    logger.info(f"Logging configured with level: {level}")


def retry_with_backoff(
    max_retries: int = 3,
    backoff_factor: float = 1.0,
    exceptions: tuple = (Exception,),
    logger: Optional[logging.Logger] = None
) -> Callable:
    """
    Decorator for retrying functions with exponential backoff.
    
    Args:
        max_retries: Maximum number of retry attempts
        backoff_factor: Factor for exponential backoff
        exceptions: Tuple of exceptions to catch and retry
        logger: Logger instance for logging retry attempts
    
    Returns:
        Decorator function
    """
    def decorator(func: Callable) -> Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs) -> Any:
            last_exception = None
            
            for attempt in range(max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    last_exception = e
                    
                    if attempt == max_retries:
                        # Last attempt failed, re-raise the exception
                        if logger:
                            logger.error(f"Function {func.__name__} failed after {max_retries} retries: {e}")
                        raise
                    
                    # Calculate backoff delay
                    delay = backoff_factor * (2 ** attempt)
                    
                    if logger:
                        logger.warning(f"Function {func.__name__} failed (attempt {attempt + 1}/{max_retries + 1}): {e}. Retrying in {delay:.2f}s...")
                    
                    time.sleep(delay)
            
            # This should never be reached, but just in case
            if last_exception:
                raise last_exception
                
        return wrapper
    return decorator


def validate_connection_string(connection_string: str) -> bool:
    """
    Validate a connection string format.
    
    Args:
        connection_string: Connection string to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not connection_string or not connection_string.strip():
        return False
    
    # Basic validation - could be enhanced based on specific requirements
    required_parts = ['account', 'user']
    return all(part in connection_string.lower() for part in required_parts)


def sanitize_identifier(identifier: str) -> str:
    """
    Sanitize database identifier for safe use in queries.
    
    Args:
        identifier: Database identifier to sanitize
        
    Returns:
        Sanitized identifier
    """
    if not identifier:
        return ""
    
    # Remove potentially dangerous characters
    sanitized = identifier.replace('"', '').replace("'", "").replace(";", "")
    
    # Ensure it's a valid identifier
    if not sanitized.replace('_', '').replace('-', '').isalnum():
        raise ValueError(f"Invalid identifier: {identifier}")
    
    return sanitized


def format_size(size_bytes: int) -> str:
    """
    Format byte size into human-readable format.
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Human-readable size string
    """
    if size_bytes == 0:
        return "0 B"
    
    size_names = ["B", "KB", "MB", "GB", "TB"]
    i = 0
    
    while size_bytes >= 1024 and i < len(size_names) - 1:
        size_bytes /= 1024.0
        i += 1
    
    return f"{size_bytes:.2f} {size_names[i]}"


def safe_get_env_var(var_name: str, default: Any = None, required: bool = False) -> Any:
    """
    Safely get environment variable with validation.
    
    Args:
        var_name: Environment variable name
        default: Default value if not found
        required: Whether the variable is required
        
    Returns:
        Environment variable value or default
        
    Raises:
        ValueError: If required variable is not found
    """
    value = os.getenv(var_name, default)
    
    if required and value is None:
        raise ValueError(f"Required environment variable '{var_name}' not found")
    
    return value


def timestamp_to_datetime(timestamp: Any) -> Optional[datetime]:
    """
    Convert various timestamp formats to datetime.
    
    Args:
        timestamp: Timestamp in various formats
        
    Returns:
        datetime object or None if conversion fails
    """
    if timestamp is None:
        return None
    
    if isinstance(timestamp, datetime):
        return timestamp
    
    if isinstance(timestamp, (int, float)):
        try:
            # Assume Unix timestamp
            return datetime.fromtimestamp(timestamp)
        except (ValueError, OSError):
            return None
    
    if isinstance(timestamp, str):
        try:
            # Try to parse ISO format
            return datetime.fromisoformat(timestamp.replace('Z', '+00:00'))
        except ValueError:
            try:
                # Try to parse as Unix timestamp
                return datetime.fromtimestamp(float(timestamp))
            except (ValueError, OSError):
                return None
    
    return None


def create_urn(platform: str, *parts: str) -> str:
    """
    Create a URN (Uniform Resource Name) for DataHub entities.
    
    Args:
        platform: Platform name
        *parts: Parts of the URN
        
    Returns:
        Formatted URN string
    """
    sanitized_parts = [sanitize_identifier(part) for part in parts if part]
    return f"urn:li:dataset:(urn:li:dataPlatform:{platform},{'.'.join(sanitized_parts)},PROD)"


def chunk_list(lst: list, chunk_size: int) -> list:
    """
    Split a list into chunks of specified size.
    
    Args:
        lst: List to chunk
        chunk_size: Size of each chunk
        
    Returns:
        List of chunks
    """
    if chunk_size <= 0:
        raise ValueError("Chunk size must be positive")
    
    return [lst[i:i + chunk_size] for i in range(0, len(lst), chunk_size)]


def measure_execution_time(func: Callable) -> Callable:
    """
    Decorator to measure and log execution time of a function.
    
    Args:
        func: Function to measure
        
    Returns:
        Decorated function
    """
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start_time = time.time()
        logger = logging.getLogger(func.__module__)
        
        try:
            result = func(*args, **kwargs)
            execution_time = time.time() - start_time
            logger.info(f"Function {func.__name__} executed in {execution_time:.2f} seconds")
            return result
        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(f"Function {func.__name__} failed after {execution_time:.2f} seconds: {e}")
            raise
    
    return wrapper


def validate_url(url: str) -> bool:
    """
    Validate URL format.
    
    Args:
        url: URL to validate
        
    Returns:
        True if valid, False otherwise
    """
    if not url:
        return False
    
    return url.startswith(('http://', 'https://'))


def truncate_string(s: str, max_length: int = 100) -> str:
    """
    Truncate string to specified length with ellipsis.
    
    Args:
        s: String to truncate
        max_length: Maximum length
        
    Returns:
        Truncated string
    """
    if not s:
        return ""
    
    if len(s) <= max_length:
        return s
    
    return s[:max_length - 3] + "..."


class Timer:
    """Context manager for timing code execution."""
    
    def __init__(self, name: str = "Operation", logger: Optional[logging.Logger] = None):
        self.name = name
        self.logger = logger or logging.getLogger(__name__)
        self.start_time = None
        self.end_time = None
    
    def __enter__(self):
        self.start_time = time.time()
        self.logger.info(f"Starting {self.name}")
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.end_time = time.time()
        duration = self.end_time - self.start_time
        
        if exc_type is None:
            self.logger.info(f"Completed {self.name} in {duration:.2f} seconds")
        else:
            self.logger.error(f"Failed {self.name} after {duration:.2f} seconds: {exc_val}")
    
    def get_duration(self) -> float:
        """Get duration of the timed operation."""
        if self.start_time and self.end_time:
            return self.end_time - self.start_time
        return 0.0


def get_config_path(filename: str = "config.json") -> str:
    """
    Get the configuration file path, checking multiple locations.
    
    Args:
        filename: Configuration filename
        
    Returns:
        Path to configuration file
    """
    # Check current directory
    if os.path.exists(filename):
        return filename
    
    # Check user's home directory
    home_path = os.path.expanduser(f"~/.snowflake-datahub/{filename}")
    if os.path.exists(home_path):
        return home_path
    
    # Check system configuration directory
    system_path = f"/etc/snowflake-datahub/{filename}"
    if os.path.exists(system_path):
        return system_path
    
    # Return default path in current directory
    return filename
