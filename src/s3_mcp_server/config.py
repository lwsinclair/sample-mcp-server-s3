"""
Configuration module for S3 MCP server.
Centralizes configuration values and content type mappings.
"""

import os
import re
from typing import Dict, Set, List, Optional
from pydantic import BaseModel, Field

class S3Config(BaseModel):
    """Configuration for S3 operations and server settings."""
    
    # AWS configuration
    region_name: str = Field(
        default="us-east-1", 
        description="AWS region for S3 operations"
    )
    
    # Resource limits and configuration
    max_buckets: int = Field(
        default=5, 
        description="Maximum number of buckets to process"
    )
    max_objects: int = Field(
        default=1000, 
        description="Maximum number of objects to return per bucket"
    )
    max_concurrent_buckets: int = Field(
        default=3, 
        description="Maximum number of concurrent bucket operations"
    )
    max_retries: int = Field(
        default=3, 
        description="Maximum number of retries for S3 operations"
    )
    
    # Timeouts (in seconds)
    connect_timeout: int = Field(
        default=5, 
        description="Connection timeout for S3 operations"
    )
    read_timeout: int = Field(
        default=60, 
        description="Read timeout for S3 operations"
    )
    
    # S3 bucket configuration
    configured_buckets: Optional[List[str]] = Field(
        default=None, 
        description="List of configured bucket names"
    )
    
    # Content type mappings for specific file types
    content_type_mapping: Dict[str, str] = Field(
        default_factory=lambda: {
            "application/vnd.openxmlformats-officedocument.wordprocessingml.document": "application/markdown",
            "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet": "application/csv",
            "application/vnd.ms-excel": "application/csv"
        },
        description="Mapping of content types for specific file formats"
    )
    
    # Extensions for text files
    text_extensions: Set[str] = Field(
        default_factory=lambda: {
            '.txt', '.log', '.json', '.xml', '.yml', '.yaml', '.md',
            '.csv', '.ini', '.conf', '.py', '.js', '.html', '.css',
            '.sh', '.bash', '.cfg', '.properties'
        },
        description="File extensions considered as text files"
    )
    
    @classmethod
    def from_env(cls) -> "S3Config":
        """
        Create configuration from environment variables.
        
        Returns:
            S3Config: Configuration instance with values from environment
        """
        # Get max buckets from environment or use default
        max_buckets = int(os.getenv('S3_MAX_BUCKETS', '5'))
        max_objects = int(os.getenv('S3_MAX_OBJECTS', '1000'))
        max_concurrent_buckets = int(os.getenv('S3_MAX_CONCURRENT_BUCKETS', '3'))
        connect_timeout = int(os.getenv('S3_CONNECT_TIMEOUT', '5'))
        read_timeout = int(os.getenv('S3_READ_TIMEOUT', '60'))
        region_name = os.getenv('AWS_REGION', 'us-east-1')
        
        # Get configured buckets
        buckets = []
        bucket_list = os.getenv('S3_BUCKETS')
        if bucket_list:
            buckets = [b.strip() for b in bucket_list.split(',')]
        else:
            i = 1
            while True:
                bucket = os.getenv(f'S3_BUCKET_{i}')
                if not bucket:
                    break
                buckets.append(bucket.strip())
                i += 1
                
        return cls(
            region_name=region_name,
            max_buckets=max_buckets,
            max_objects=max_objects,
            max_concurrent_buckets=max_concurrent_buckets,
            max_retries=int(os.getenv('S3_MAX_RETRIES', '3')),
            connect_timeout=connect_timeout,
            read_timeout=read_timeout,
            configured_buckets=buckets if buckets else None
        )

def is_text_file(key: str, config: S3Config) -> bool:
    """
    Determine if a file is text-based by its extension
    
    Args:
        key: The object key/filename to check
        config: The S3Config instance containing text extensions
        
    Returns:
        bool: True if the file is a text file, False otherwise
    """
    return any(key.lower().endswith(ext) for ext in config.text_extensions)

def sanitize_s3_path(path: str) -> str:
    """
    Sanitize an S3 path to prevent path traversal attacks
    
    Args:
        path: The S3 path to sanitize
        
    Returns:
        str: The sanitized path
    """
    # Remove any double dots that could lead to path traversal
    path = re.sub(r'\.\.', '', path)
    # Remove any leading or trailing slashes
    path = path.strip('/')
    # Ensure the path only contains valid characters
    if not re.match(r'^[a-zA-Z0-9_\-\.\/]+$', path):
        raise ValueError("Invalid characters in S3 path")
    return path