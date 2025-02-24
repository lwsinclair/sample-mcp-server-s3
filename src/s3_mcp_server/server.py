import asyncio
import base64
import logging
import os
import re
from importlib.resources import contents
from typing import List, Optional, Dict, Any, Union
from urllib.parse import unquote

# Third-party imports
import boto3
from dotenv import load_dotenv
from mcp.server import NotificationOptions, Server, McpError
from mcp.server.models import InitializationOptions
import mcp.server.stdio
from mcp.types import Resource, LoggingLevel, EmptyResult, Tool, TextContent, ImageContent, EmbeddedResource, BlobResourceContents, ReadResourceResult
from pydantic import AnyUrl, Field, BaseModel

# Local imports
from .resources.s3_resource import S3Resource
from .config import S3Config

# Load environment variables first
load_dotenv()

# Configure logging with safer default level
log_level = os.getenv('LOG_LEVEL', 'INFO').upper()
logging.basicConfig(level=getattr(logging, log_level))
logger = logging.getLogger("mcp_s3_server")

# Initialize server
server = Server("s3_service")

# Load configuration from environment variables
config = S3Config.from_env()

# Get AWS profile from environment
aws_profile = os.getenv('AWS_PROFILE')

# Initialize S3 resource with config and profile
s3_resource = S3Resource(config=config, profile_name=aws_profile)

# Create a boto3 session with profile
boto3_session = boto3.Session(profile_name=aws_profile)

# Create a single boto3 client for tools
boto3_s3_client = boto3_session.client(
    's3',
    region_name=config.region_name,
    config=boto3.session.Config(
        retries={'max_attempts': config.max_retries, 'mode': 'adaptive'},
        connect_timeout=config.connect_timeout,
        read_timeout=config.read_timeout
    )
)

@server.set_logging_level()
async def set_logging_level(level: LoggingLevel) -> EmptyResult:
    logger.setLevel(level.lower())
    await server.request_context.session.send_log_message(
        level="info",
        data=f"Log level set to {level}",
        logger="mcp_s3_server"
    )
    return EmptyResult()

@server.list_resources()
async def list_resources(start_after: Optional[str] = None) -> List[Resource]:
    """
    List S3 buckets and their contents as resources with pagination
    
    Args:
        start_after: Start listing after this resource URI
        
    Returns:
        List of S3 resources matching the criteria
        
    Raises:
        McpError: When resource listing fails
    """
    resources = []
    cursor_info = None
    max_resources = 1000  # Maximum number of resources to return in one response
    
    # If start_after is provided, try to extract cursor information
    if start_after and start_after.startswith("s3://"):
        try:
            # Parse the cursor information from the start_after URI
            # Format: s3://bucket-name/prefix?cursor=last-key
            parts = start_after[5:].split('?', 1)
            path = parts[0]
            bucket_parts = path.split('/', 1)
            cursor_info = {
                'bucket': bucket_parts[0],
                'prefix': bucket_parts[1] if len(bucket_parts) > 1 else '',
                'last_key': parts[1].split('=', 1)[1] if len(parts) > 1 and parts[1].startswith('cursor=') else None
            }
            logger.debug(f"Parsed cursor info: {cursor_info}")
        except Exception as e:
            logger.warning(f"Failed to parse cursor information from {start_after}: {str(e)}")
            # Continue with default behavior
    
    logger.debug("Starting to list resources")
    logger.debug(f"Configured buckets: {s3_resource.configured_buckets}")

    try:
        # Get buckets
        try:
            buckets = await s3_resource.list_buckets(
                start_after=cursor_info['bucket'] if cursor_info else None
            )
            logger.debug(f"Processing {len(buckets)} buckets (max: {config.max_buckets})")
        except S3ResourceError as e:
            logger.error(f"Failed to list buckets: {str(e)}")
            raise McpError(f"Failed to list buckets: {str(e)}", code=500)

        # Define bucket processing function with proper resource type detection
        async def process_bucket(bucket):
            nonlocal resources
            bucket_name = bucket['Name']
            logger.debug(f"Processing bucket: {bucket_name}")
            bucket_resources = []

            try:
                # Determine prefix and continuation token
                prefix = ""
                if cursor_info and cursor_info['bucket'] == bucket_name:
                    prefix = cursor_info['prefix']
                
                # List objects in the bucket with a reasonable limit
                objects = await s3_resource.list_objects(
                    bucket_name, 
                    prefix=prefix,
                    max_keys=config.max_objects
                )

                for obj in objects:
                    if 'Key' not in obj or obj['Key'].endswith('/'):
                        continue
                        
                    # Skip objects before the cursor if needed
                    if cursor_info and cursor_info['bucket'] == bucket_name and cursor_info['last_key']:
                        if obj['Key'] <= cursor_info['last_key']:
                            continue
                    
                    object_key = obj['Key']
                    
                    # Determine mime type based on file extension
                    is_text = s3_resource.is_text_file(object_key)
                    # Default mime types for text and binary files
                    mime_type = "text/plain" if is_text else "application/octet-stream"
                    
                    # More specific mime type detection based on extension
                    if object_key.lower().endswith('.pdf'):
                        mime_type = "application/pdf"
                    elif object_key.lower().endswith('.md'):
                        mime_type = "text/markdown"
                    elif object_key.lower().endswith('.json'):
                        mime_type = "application/json"
                    elif object_key.lower().endswith('.csv'):
                        mime_type = "text/csv"

                    # Create resource record
                    resource = Resource(
                        uri=f"s3://{bucket_name}/{object_key}",
                        name=object_key,
                        mimeType=mime_type
                    )
                    bucket_resources.append(resource)
                    
                    # Check if we've reached the resource limit
                    if len(resources) + len(bucket_resources) >= max_resources:
                        # Add what we have and stop processing
                        resources.extend(bucket_resources)
                        return True  # Signal we've hit the limit
                
                # Add all resources from this bucket
                resources.extend(bucket_resources)
                return False  # Signal we haven't hit the limit

            except S3BucketAccessError as e:
                logger.warning(f"Access denied to bucket {bucket_name}: {str(e)}")
                return False  # Continue with other buckets
            except Exception as e:
                logger.error(f"Error listing objects in bucket {bucket_name}: {str(e)}", exc_info=True)
                return False  # Continue with other buckets

        # Use semaphore to limit concurrent bucket processing
        semaphore = asyncio.Semaphore(config.max_concurrent_buckets)
        
        # Process buckets concurrently with limit control
        limit_reached = False
        for bucket in buckets:
            if limit_reached:
                break
                
            async with semaphore:
                limit_reached = await process_bucket(bucket)

    except McpError:
        # Let McpError pass through
        raise
    except Exception as e:
        logger.error(f"Unexpected error listing resources: {str(e)}", exc_info=True)
        raise McpError(f"Failed to list resources: {str(e)}", code=500)

    logger.info(f"Returning {len(resources)} resources")
    return resources



@server.read_resource()
async def read_resource(uri: AnyUrl) -> str:
    """
    Read content from an S3 resource and return structured response
    
    Args:
        uri: The S3 URI to read (s3://bucket-name/object-key)
        
    Returns:
        Base64 encoded string representation of the object content
        
    Raises:
        ValueError: For invalid URI format or object access issues
        McpError: For MCP protocol specific errors
    """
    uri_str = str(uri)
    logger.debug(f"Reading resource: {uri_str}")

    # Validate URI format
    if not uri_str.startswith("s3://"):
        logger.error(f"Invalid URI scheme: {uri_str}")
        raise ValueError("Invalid URI scheme. Must start with s3://")

    try:
        # Parse and sanitize the S3 URI
        path = uri_str[5:]  # Remove "s3://"
        path = unquote(path)  # Decode URL-encoded characters
        
        # Sanitize path to prevent traversal attacks
        try:
            from .config import sanitize_s3_path
            path = sanitize_s3_path(path)
        except ValueError as e:
            logger.error(f"Path sanitization failed: {str(e)}")
            raise ValueError(f"Invalid S3 path: {str(e)}")
            
        parts = path.split("/", 1)

        if len(parts) < 2:
            logger.error(f"Invalid S3 URI format: {uri_str}")
            raise ValueError("Invalid S3 URI format. Must be s3://bucket-name/object-key")

        bucket_name = parts[0]
        key = parts[1]

        logger.debug(f"Attempting to read - Bucket: {bucket_name}, Key: {key}")

        # Get object from S3
        try:
            response = await s3_resource.get_object(bucket_name, key)
        except S3BucketAccessError as e:
            logger.error(f"Access denied: {str(e)}")
            raise McpError(str(e), code=403)
        except S3ObjectNotFoundError as e:
            logger.error(f"Object not found: {str(e)}")
            # Try to find similar objects for debugging purposes
            try:
                prefix = key.split('/')[0] if '/' in key else ''
                objects = await s3_resource.list_objects(bucket_name, prefix=prefix)
                similar_objects = [obj['Key'] for obj in objects if 'Key' in obj]
                logger.debug(f"Similar objects found: {similar_objects}")
            except Exception:
                pass
            raise McpError(f"Resource not found: {uri_str}", code=404)
        except S3StreamingError as e:
            logger.error(f"Streaming error: {str(e)}")
            raise McpError(f"Failed to stream resource: {str(e)}", code=500)
        except S3ResourceError as e:
            logger.error(f"S3 resource error: {str(e)}")
            raise McpError(f"S3 error: {str(e)}", code=500)
            
        # Process content type
        content_type = response.get("ContentType", "")
        logger.debug(f"Content type from S3: {content_type}")
        
        # Use content type mapping from config
        export_mime_type = config.content_type_mapping.get(content_type, content_type)
        logger.debug(f"Mapped content type: {export_mime_type}")

        # Process the data
        if 'Body' not in response:
            logger.error("No data in response body")
            raise McpError("No data in response body", code=500)
            
        data = response['Body']
        if not isinstance(data, bytes):
            logger.error("Unexpected data type in response body")
            raise McpError("Unexpected data type in response body", code=500)

        # Encode the data based on file type
        try:
            if s3_resource.is_text_file(key):
                # Base64 encode the content as required by MCP protocol
                encoded_content = base64.b64encode(data).decode('utf-8')
                return encoded_content
            else:
                # For binary files
                encoded_content = base64.b64encode(data).decode('utf-8')
                
                # Create a structured result
                result = ReadResourceResult(
                    contents=[
                        BlobResourceContents(
                            blob=encoded_content,
                            uri=uri_str,
                            mimeType=export_mime_type
                        )
                    ]
                )
                return encoded_content
        except Exception as e:
            logger.error(f"Error encoding content: {str(e)}", exc_info=True)
            raise McpError(f"Error processing resource content: {str(e)}", code=500)

    except (ValueError, McpError):
        # Let these pass through as they're already properly formatted
        raise
    except Exception as e:
        # Catch all other exceptions and format appropriately
        logger.error(f"Unexpected error reading resource {uri_str}: {str(e)}", exc_info=True)
        raise McpError(f"Failed to read resource: {str(e)}", code=500)


@server.list_tools()
async def handle_list_tools() -> list[Tool]:
    return [
        Tool(
            name="ListBuckets", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListBuckets.html
            description="Returns a list of all buckets owned by the authenticated sender of the request. To grant IAM permission to use this operation, you must add the s3:ListAllMyBuckets policy action.",
            inputSchema={
                "type": "object",
                "properties": {
                    "ContinuationToken": {"type": "string", "description": "ContinuationToken indicates to Amazon S3 that the list is being continued on this bucket with a token. ContinuationToken is obfuscated and is not a real key. You can use this ContinuationToken for pagination of the list results. Length Constraints: Minimum length of 0. Maximum length of 1024."},
                    "MaxBuckets": {"type": "integer", "description": "Maximum number of buckets to be returned in response. When the number is more than the count of buckets that are owned by an AWS account, return all the buckets in response. Valid Range: Minimum value of 1. Maximum value of 10000."},
                },
                "required": [],
            },
        ),
        Tool(
            name="ListObjectsV2", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_ListObjectsV2.html
            description="Returns some or all (up to 1,000) of the objects in a bucket with each request. You can use the request parameters as selection criteria to return a subset of the objects in a bucket. To get a list of your buckets, see ListBuckets.",
            inputSchema={
                "type": "object",
                "properties": {
                    "Bucket": {"type": "string", "description": "When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3)."},
                    "ContinuationToken": {"type": "string", "description": "ContinuationToken indicates to Amazon S3 that the list is being continued on this bucket with a token. ContinuationToken is obfuscated and is not a real key. You can use this ContinuationToken for pagination of the list results."},
                    "EncodingType": {"type": "string", "description": "Encoding type used by Amazon S3 to encode the object keys in the response. Responses are encoded only in UTF-8. An object key can contain any Unicode character. However, the XML 1.0 parser can't parse certain characters, such as characters with an ASCII value from 0 to 10. For characters that aren't supported in XML 1.0, you can add this parameter to request that Amazon S3 encode the keys in the response."},
                    "FetchOwner": {"type": "boolean", "description": "The owner field is not present in ListObjectsV2 by default. If you want to return the owner field with each key in the result, then set the FetchOwner field to true."},
                    "MaxKeys": {"type": "integer", "description": "Sets the maximum number of keys returned in the response. By default, the action returns up to 1,000 key names. The response might contain fewer keys but will never contain more."},
                    "Prefix": {"type": "string", "description": "Limits the response to keys that begin with the specified prefix."},
                    "StartAfter": {"type": "string", "description": "StartAfter is where you want Amazon S3 to start listing from. Amazon S3 starts listing after this specified key. StartAfter can be any key in the bucket."}
                },
                "required": ["Bucket"],
            },
        ),
        Tool(
            name="GetObject", # https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html
            description="Retrieves an object from Amazon S3. In the GetObject request, specify the full key name for the object. General purpose buckets - Both the virtual-hosted-style requests and the path-style requests are supported. For a virtual hosted-style request example, if you have the object photos/2006/February/sample.jpg, specify the object key name as /photos/2006/February/sample.jpg. For a path-style request example, if you have the object photos/2006/February/sample.jpg in the bucket named examplebucket, specify the object key name as /examplebucket/photos/2006/February/sample.jpg. Directory buckets - Only virtual-hosted-style requests are supported. For a virtual hosted-style request example, if you have the object photos/2006/February/sample.jpg in the bucket named examplebucket--use1-az5--x-s3, specify the object key name as /photos/2006/February/sample.jpg. Also, when you make requests to this API operation, your requests are sent to the Zonal endpoint. These endpoints support virtual-hosted-style requests in the format https://bucket_name.s3express-az_id.region.amazonaws.com/key-name . Path-style requests are not supported.",
            inputSchema={
                "type": "object",
                "properties": {
                    "Bucket": {"type": "string", "description": "Directory buckets - When you use this operation with a directory bucket, you must use virtual-hosted-style requests in the format Bucket_name.s3express-az_id.region.amazonaws.com. Path-style requests are not supported. Directory bucket names must be unique in the chosen Availability Zone. Bucket names must follow the format bucket_base_name--az-id--x-s3 (for example, DOC-EXAMPLE-BUCKET--usw2-az1--x-s3)."},
                    "Key": {"type": "string", "description": "Key of the object to get. Length Constraints: Minimum length of 1."},
                    "Range": {"type": "string", "description": "Downloads the specified byte range of an object."},
                    "VersionId": {"type": "string", "description": "Version ID used to reference a specific version of the object. By default, the GetObject operation returns the current version of an object. To return a different version, use the versionId subresource."},
                    "PartNumber": {"type": "integer", "description": "Part number of the object being read. This is a positive integer between 1 and 10,000. Effectively performs a 'ranged' GET request for the part specified. Useful for downloading just a part of an object."},
                },
                "required": ["Bucket", "Key"]
            }
        )
    ]

class ToolError(Exception):
    """Exception raised when a tool encounters an error"""
    def __init__(self, message: str, status_code: int = 500):
        self.message = message
        self.status_code = status_code
        super().__init__(message)

@server.call_tool()
async def handle_call_tool(
    name: str, arguments: dict | None
) -> list[TextContent | ImageContent | EmbeddedResource]:
    """
    Handle tool calls by delegating to appropriate AWS S3 operations
    
    Args:
        name: Name of the tool to call
        arguments: Dictionary of arguments for the tool
        
    Returns:
        List of content objects representing the tool's response
        
    Raises:
        McpError: When the tool execution fails
    """
    # Initialize arguments if None
    arguments = arguments or {}
    
    # Validate input parameters
    try:
        _validate_tool_arguments(name, arguments)
    except ToolError as e:
        logger.warning(f"Tool validation error for {name}: {e.message}")
        return [
            TextContent(
                type="text",
                text=f"Error ({e.status_code}): {e.message}"
            )
        ]
        
    # Sanitize sensitive information from logs
    safe_arguments = _sanitize_arguments(arguments)
    logger.info(f"Executing tool {name} with arguments: {safe_arguments}")
    
    try:
        # Route to the appropriate tool handler
        match name:
            case "ListBuckets":
                return await _handle_list_buckets(arguments)
            case "ListObjectsV2":
                return await _handle_list_objects_v2(arguments)
            case "GetObject":
                return await _handle_get_object(arguments)
            case _:
                # Unknown tool
                raise ToolError(f"Unknown tool: {name}", 400)
                
    except ToolError as e:
        # Handle expected tool errors
        logger.warning(f"Tool execution error for {name}: {e.message}")
        return [
            TextContent(
                type="text",
                text=f"Error ({e.status_code}): {e.message}"
            )
        ]
    except Exception as e:
        # Handle unexpected errors
        error_id = f"err-{id(e)}"
        logger.error(f"Unexpected error in tool {name} [{error_id}]: {str(e)}", exc_info=True)
        return [
            TextContent(
                type="text",
                text=f"Error (500): An unexpected error occurred. Error ID: {error_id}"
            )
        ]

def _validate_tool_arguments(name: str, arguments: dict) -> None:
    """Validate tool arguments based on tool schema"""
    match name:
        case "ListBuckets":
            # ListBuckets has no required arguments
            if 'MaxBuckets' in arguments and not isinstance(arguments['MaxBuckets'], int):
                raise ToolError("MaxBuckets must be an integer", 400)
                
        case "ListObjectsV2":
            # Check required arguments
            if 'Bucket' not in arguments or not arguments['Bucket']:
                raise ToolError("Bucket is required", 400)
                
            # Check if bucket is in configured list
            if s3_resource.configured_buckets and arguments['Bucket'] not in s3_resource.configured_buckets:
                raise ToolError(f"Bucket {arguments['Bucket']} is not in the configured bucket list", 403)
                
        case "GetObject":
            # Check required arguments
            if 'Bucket' not in arguments or not arguments['Bucket']:
                raise ToolError("Bucket is required", 400)
            if 'Key' not in arguments or not arguments['Key']:
                raise ToolError("Key is required", 400)
                
            # Check if bucket is in configured list
            if s3_resource.configured_buckets and arguments['Bucket'] not in s3_resource.configured_buckets:
                raise ToolError(f"Bucket {arguments['Bucket']} is not in the configured bucket list", 403)
                
            # Sanitize Key to prevent path traversal
            try:
                from .config import sanitize_s3_path
                arguments['Key'] = sanitize_s3_path(arguments['Key'])
            except ValueError as e:
                raise ToolError(f"Invalid Key: {str(e)}", 400)

def _sanitize_arguments(arguments: dict) -> dict:
    """
    Remove sensitive information from arguments for logging
    """
    # Create a copy to avoid modifying the original
    safe_args = arguments.copy()
    
    # Sanitize potential security-sensitive fields
    if 'ACL' in safe_args:
        safe_args['ACL'] = '***'
    if 'SSECustomerKey' in safe_args:
        safe_args['SSECustomerKey'] = '***'
    if 'Body' in safe_args:
        safe_args['Body'] = '***'
    
    return safe_args

async def _handle_list_buckets(arguments: dict) -> list[TextContent]:
    """Handle ListBuckets tool"""
    try:
        # Convert arguments to match API expectations
        api_args = {}
        if 'MaxBuckets' in arguments:
            # This is a custom parameter, not in the real AWS API
            max_buckets = min(int(arguments['MaxBuckets']), config.max_buckets)
        else:
            max_buckets = config.max_buckets
            
        # Call the API
        response = boto3_s3_client.list_buckets(**api_args)
        
        # Limit the number of buckets returned
        if 'Buckets' in response:
            response['Buckets'] = response['Buckets'][:max_buckets]
            
        # Format the response
        return [
            TextContent(
                type="text",
                text=str(response)
            )
        ]
    except Exception as e:
        raise ToolError(f"Error listing buckets: {str(e)}")

async def _handle_list_objects_v2(arguments: dict) -> list[TextContent]:
    """Handle ListObjectsV2 tool"""
    try:
        # Copy arguments to avoid modifying the original
        api_args = arguments.copy()
        
        # Ensure MaxKeys is within limits
        if 'MaxKeys' in api_args:
            api_args['MaxKeys'] = min(int(api_args['MaxKeys']), config.max_objects)
        else:
            api_args['MaxKeys'] = config.max_objects
            
        # Call the API
        response = boto3_s3_client.list_objects_v2(**api_args)
        
        # Format the response
        return [
            TextContent(
                type="text",
                text=str(response)
            )
        ]
    except boto3.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchBucket':
            raise ToolError(f"Bucket {arguments['Bucket']} does not exist", 404)
        elif error_code == 'AccessDenied':
            raise ToolError(f"Access denied to bucket {arguments['Bucket']}", 403)
        else:
            raise ToolError(f"AWS error: {str(e)}")
    except Exception as e:
        raise ToolError(f"Error listing objects: {str(e)}")

async def _handle_get_object(arguments: dict) -> list[TextContent]:
    """Handle GetObject tool"""
    try:
        # Copy arguments to avoid modifying the original
        api_args = arguments.copy()
        
        # Call the API
        response = boto3_s3_client.get_object(**api_args)
        
        try:
            # Get the content type
            content_type = response.get('ContentType', '')
            
            # Read the content based on content type
            is_text = any(ext in content_type for ext in [
                'text/', 'json', 'xml', 'javascript', 'html', 'css'
            ])
            
            if is_text:
                # Try to decode as text first with a safe fallback
                try:
                    file_content = response['Body'].read().decode('utf-8')
                except UnicodeDecodeError:
                    # If it fails, treat as binary
                    logger.warning(f"Failed to decode as UTF-8 despite content type {content_type}")
                    file_content = f"[Binary content with size {response.get('ContentLength', 'unknown')} bytes]"
            else:
                # For binary files, just report the size
                file_content = f"[Binary content with size {response.get('ContentLength', 'unknown')} bytes]"
                
            return [
                TextContent(
                    type="text",
                    text=str(file_content)
                )
            ]
        finally:
            # Ensure the body is closed
            if 'Body' in response and hasattr(response['Body'], 'close'):
                response['Body'].close()
                
    except boto3.exceptions.ClientError as e:
        error_code = e.response.get('Error', {}).get('Code', '')
        if error_code == 'NoSuchKey':
            raise ToolError(f"Object {arguments['Key']} not found in bucket {arguments['Bucket']}", 404)
        elif error_code == 'NoSuchBucket':
            raise ToolError(f"Bucket {arguments['Bucket']} does not exist", 404)
        elif error_code == 'AccessDenied':
            raise ToolError(f"Access denied to object {arguments['Key']} in bucket {arguments['Bucket']}", 403)
        else:
            raise ToolError(f"AWS error: {str(e)}")
    except Exception as e:
        raise ToolError(f"Error getting object: {str(e)}")

async def main():
    # Run the server using stdin/stdout streams
    async with mcp.server.stdio.stdio_server() as (read_stream, write_stream):
        await server.run(
            read_stream,
            write_stream,
            InitializationOptions(
                server_name="s3-mcp-server",
                server_version="0.1.0",
                capabilities=server.get_capabilities(
                    notification_options=NotificationOptions(),
                    experimental_capabilities={},
                ),
            ),
        )

if __name__ == "__main__":
    asyncio.run(main())