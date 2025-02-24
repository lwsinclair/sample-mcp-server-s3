import logging
import os
from typing import List, Dict, Any, Optional, Union
import aioboto3
import asyncio
from botocore.config import Config
from botocore.exceptions import ClientError, BotoCoreError

from ..config import S3Config, is_text_file

logger = logging.getLogger("s3_mcp_server")

class S3ResourceError(Exception):
    """Base exception for S3Resource errors"""
    pass

class S3BucketAccessError(S3ResourceError):
    """Exception raised when bucket access is denied"""
    pass

class S3ObjectNotFoundError(S3ResourceError):
    """Exception raised when an object is not found"""
    pass

class S3StreamingError(S3ResourceError):
    """Exception raised during streaming operations"""
    pass

class S3Resource:
    """
    S3 Resource provider that handles interactions with AWS S3 buckets.
    Part of a collection of resource providers (S3, DynamoDB, etc.) for the MCP server.
    
    This class provides methods to:
    - List S3 buckets with pagination
    - List objects in a bucket with filtering
    - Retrieve object content with streaming support
    - Check object metadata
    """

    def __init__(self, config: Optional[S3Config] = None, region_name: str = None, profile_name: str = None, max_buckets: int = 5):
        """
        Initialize S3 resource provider
        
        Args:
            config: Configuration object (takes precedence over other parameters)
            region_name: AWS region name (only used if config is None)
            profile_name: AWS profile name (only used if config is None)
            max_buckets: Maximum number of buckets to process (only used if config is None)
        """
        # Use provided config or create default
        self.config = config or S3Config(
            region_name=region_name or "us-east-1",
            max_buckets=max_buckets
        )
        
        # Configure boto3 with retries and timeouts
        self.boto_config = Config(
            retries=dict(
                max_attempts=self.config.max_retries,
                mode='adaptive'
            ),
            connect_timeout=self.config.connect_timeout,
            read_timeout=self.config.read_timeout,
            max_pool_connections=50
        )

        # Use profile name from config or environment variable
        profile_name = profile_name or os.getenv('AWS_PROFILE')
        
        # Create session with profile if specified
        self.session = aioboto3.Session(profile_name=profile_name)
        self.region_name = self.config.region_name
        self.max_buckets = self.config.max_buckets
        self.configured_buckets = self.config.configured_buckets

    async def list_buckets(self, start_after: Optional[str] = None) -> List[dict]:
        """
        List S3 buckets using async client with pagination
        
        Args:
            start_after: Start listing after this bucket name
            
        Returns:
            List of bucket dictionaries with 'Name' and 'CreationDate'
            
        Raises:
            S3ResourceError: On general AWS API errors
        """
        try:
            async with self.session.client('s3', 
                                          region_name=self.region_name, 
                                          config=self.boto_config) as s3:
                if self.configured_buckets:
                    # If buckets are configured, only return those
                    response = await s3.list_buckets()
                    all_buckets = response.get('Buckets', [])
                    configured_bucket_list = [
                        bucket for bucket in all_buckets
                        if bucket['Name'] in self.configured_buckets
                    ]

                    if start_after:
                        configured_bucket_list = [
                            b for b in configured_bucket_list
                            if b['Name'] > start_after
                        ]

                    return configured_bucket_list[:self.max_buckets]
                else:
                    # Default behavior if no buckets configured
                    response = await s3.list_buckets()
                    buckets = response.get('Buckets', [])

                    if start_after:
                        buckets = [b for b in buckets if b['Name'] > start_after]

                    return buckets[:self.max_buckets]
        except (ClientError, BotoCoreError) as e:
            logger.error(f"AWS API error listing buckets: {str(e)}")
            raise S3ResourceError(f"Failed to list buckets: {str(e)}") from e
        except Exception as e:
            logger.error(f"Unexpected error listing buckets: {str(e)}", exc_info=True)
            raise S3ResourceError(f"Unexpected error listing buckets: {str(e)}") from e

    async def list_objects(self, bucket_name: str, prefix: str = "", max_keys: int = 1000) -> List[dict]:
        """
        List objects in a specific bucket using async client with pagination
        
        Args:
            bucket_name: Name of the S3 bucket
            prefix: Object prefix for filtering
            max_keys: Maximum number of keys to return
            
        Returns:
            List of object dictionaries with metadata
            
        Raises:
            S3BucketAccessError: When bucket access is denied
            S3ResourceError: On other AWS API errors
        """
        # Validate input parameters
        if not bucket_name:
            raise ValueError("Bucket name is required")
        
        # Check if bucket is in configured list
        if self.configured_buckets and bucket_name not in self.configured_buckets:
            logger.warning(f"Bucket {bucket_name} not in configured bucket list")
            return []

        # Enforce max_keys limit from config
        max_keys = min(max_keys, self.config.max_objects)
        
        try:
            async with self.session.client('s3', 
                                          region_name=self.region_name,
                                          config=self.boto_config) as s3:
                response = await s3.list_objects_v2(
                    Bucket=bucket_name,
                    Prefix=prefix,
                    MaxKeys=max_keys
                )
                return response.get('Contents', [])
        except ClientError as e:
            error_code = e.response.get('Error', {}).get('Code', '')
            if error_code == 'AccessDenied':
                logger.error(f"Access denied to bucket {bucket_name}")
                raise S3BucketAccessError(f"Access denied to bucket {bucket_name}")
            elif error_code == 'NoSuchBucket':
                logger.error(f"Bucket {bucket_name} does not exist")
                return []
            else:
                logger.error(f"AWS API error listing objects in bucket {bucket_name}: {str(e)}")
                raise S3ResourceError(f"Failed to list objects in bucket {bucket_name}: {str(e)}") from e
        except Exception as e:
            logger.error(f"Unexpected error listing objects in bucket {bucket_name}: {str(e)}", exc_info=True)
            raise S3ResourceError(f"Unexpected error listing objects: {str(e)}") from e

    async def get_object(self, bucket_name: str, key: str, max_retries: Optional[int] = None) -> Dict[str, Any]:
        """
        Get object from S3 using streaming to handle large files and PDFs reliably.
        The method reads the stream in chunks and concatenates them before returning.
        
        Args:
            bucket_name: Name of the S3 bucket
            key: Key of the object to retrieve
            max_retries: Maximum number of retries (defaults to config value)
            
        Returns:
            Dictionary with object data and metadata
            
        Raises:
            S3BucketAccessError: When bucket access is denied
            S3ObjectNotFoundError: When object is not found
            S3StreamingError: When streaming fails
            S3ResourceError: On other AWS API errors
        """
        # Validate input parameters
        if not bucket_name or not key:
            raise ValueError("Both bucket_name and key are required")
        
        # Check if bucket is in configured list
        if self.configured_buckets and bucket_name not in self.configured_buckets:
            raise S3BucketAccessError(f"Bucket {bucket_name} not in configured bucket list")

        # Use config max_retries if not specified
        max_retries = max_retries if max_retries is not None else self.config.max_retries
        
        attempt = 0
        last_exception = None
        chunk_size = 69 * 1024  # Using same chunk size as example for proven performance

        while attempt < max_retries:
            try:
                async with self.session.client('s3',
                                               region_name=self.region_name,
                                               config=self.boto_config) as s3:

                    # Get the object and its stream
                    response = await s3.get_object(Bucket=bucket_name, Key=key)
                    
                    # Set a timeout for the streaming operation
                    stream_timeout = asyncio.create_task(asyncio.sleep(self.config.read_timeout))
                    read_task = asyncio.create_task(self._read_stream(response['Body']))
                    
                    # Wait for either task to complete
                    done, pending = await asyncio.wait(
                        [stream_timeout, read_task],
                        return_when=asyncio.FIRST_COMPLETED
                    )
                    
                    # Cancel the pending task
                    for task in pending:
                        task.cancel()
                        
                    # If timeout occurred
                    if stream_timeout in done:
                        raise S3StreamingError(f"Streaming timeout for {bucket_name}/{key}")
                        
                    # Get the result from read_task
                    data = read_task.result()
                    
                    # Replace the stream with the complete data
                    response['Body'] = data
                    return response

            except ClientError as e:
                last_exception = e
                error_code = e.response.get('Error', {}).get('Code', '')
                
                if error_code == 'NoSuchKey':
                    logger.error(f"Object {key} not found in bucket {bucket_name}")
                    raise S3ObjectNotFoundError(f"Object {key} not found in bucket {bucket_name}")
                elif error_code == 'AccessDenied':
                    logger.error(f"Access denied to object {key} in bucket {bucket_name}")
                    raise S3BucketAccessError(f"Access denied to object {key} in bucket {bucket_name}")
                
                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Attempt {attempt} failed, retrying in {wait_time} seconds: {str(e)}")
                    await asyncio.sleep(wait_time)
                continue
            except asyncio.CancelledError:
                logger.error(f"Operation cancelled for {bucket_name}/{key}")
                raise S3StreamingError(f"Operation was cancelled")
            except Exception as e:
                last_exception = e
                logger.error(f"Error retrieving object {key} from bucket {bucket_name}: {str(e)}", exc_info=True)
                
                attempt += 1
                if attempt < max_retries:
                    wait_time = 2 ** attempt
                    logger.warning(f"Attempt {attempt} failed, retrying in {wait_time} seconds: {str(e)}")
                    await asyncio.sleep(wait_time)
                continue

        err_msg = f"Failed to get object after {max_retries} retries"
        logger.error(err_msg)
        if last_exception:
            raise S3ResourceError(err_msg) from last_exception
        else:
            raise S3ResourceError(err_msg)
            
    async def _read_stream(self, stream) -> bytes:
        """
        Read an async stream completely
        
        Args:
            stream: The async stream to read
            
        Returns:
            bytes: The complete stream data
        """
        chunks = []
        try:
            async for chunk in stream:
                chunks.append(chunk)
            return b''.join(chunks)
        except Exception as e:
            logger.error(f"Error reading stream: {str(e)}", exc_info=True)
            raise S3StreamingError(f"Error reading stream: {str(e)}") from e
        finally:
            # Ensure stream is closed
            if hasattr(stream, 'close'):
                await stream.close()

    def is_text_file(self, key: str) -> bool:
        """
        Determine if a file is text-based by its extension
        
        Args:
            key: The object key/filename to check
            
        Returns:
            bool: True if the file is a text file, False otherwise
        """
        return is_text_file(key, self.config)