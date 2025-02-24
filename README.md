# Sample S3 Model Context Protocol Server

An MCP server implementation for retrieving  data such as PDF's from S3.

## Features
### Resources
Expose AWS S3 Data through **Resources**. (think of these sort of like GET endpoints; they are used to load information into the LLM's context). Currently only **PDF** documents supported and limited to **1000** objects.


### Tools
- **ListBuckets**
  - Returns a list of all buckets owned by the authenticated sender of the request
- **ListObjectsV2**
  - Returns some or all (up to 1,000) of the objects in a bucket with each request
- **GetObject**
  - Retrieves an object from Amazon S3. In the GetObject request, specify the full key name for the object. General purpose buckets - Both the virtual-hosted-style requests and the path-style requests are supported


## Configuration

### Environment Configuration
1. Create a `.env` file in the root directory of the project by copying the provided `env.example` file:
   ```bash
   cp env.example .env
   ```

2. Edit the `.env` file to configure the following settings:
   - AWS Region: `AWS_REGION=us-east-1`
   - S3 Buckets: Either use a comma-separated list `S3_BUCKETS=bucket1,bucket2,bucket3` or individual entries `S3_BUCKET_1=mybucket1`
   - Resource Limits: Set `S3_MAX_BUCKETS`, `S3_MAX_OBJECTS`, and `S3_MAX_CONCURRENT_BUCKETS` 
   - Performance Settings: Configure `S3_MAX_RETRIES`, `S3_CONNECT_TIMEOUT`, and `S3_READ_TIMEOUT`
   - Logging: Set `LOG_LEVEL` to DEBUG, INFO, WARNING, ERROR, or CRITICAL

### Setting up AWS Credentials
1. Obtain AWS access key ID, secret access key, and region from the AWS Management Console and configure credentials using one of the standard AWS credentials providers:
   - Environment variables: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`
   - Shared credentials file (`~/.aws/credentials`)
   - Named profiles (set `AWS_PROFILE` environment variable)
   - IAM Instance Profiles (for EC2)
   - Web Identity Token (for EKS)
   - Assume Role (for cross-account access)

2. You can specify a named AWS profile by either:
   - Setting the `AWS_PROFILE` environment variable
   - Uncommenting and setting the `AWS_PROFILE` line in your `.env` file

3. Ensure these credentials have appropriate READ/WRITE permissions for S3.

For more details, see the [AWS SDK for Python (Boto3) credentials documentation](https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html).

### Usage with Claude Desktop

#### Claude Desktop

On MacOS: `~/Library/Application\ Support/Claude/claude_desktop_config.json`
On Windows: `%APPDATA%/Claude/claude_desktop_config.json`

<details>
  <summary>Development/Unpublished Servers Configuration</summary>

```json
{
  "mcpServers": {
    "s3-mcp-server": {
      "command": "uv",
      "args": [
        "--directory",
        "/Users/user/generative_ai/model_context_protocol/s3-mcp-server",
        "run",
        "s3-mcp-server"
      ]
    }
  }
}
```

</details>

<details>
  <summary>Published Servers Configuration</summary>

```json
{
  "mcpServers": {
    "s3-mcp-server": {
      "command": "uvx",
      "args": [
        "s3-mcp-server"
      ]
    }
  }
}
  ```
</details>

## Development

### Building and Publishing

To prepare the package for distribution:

1. Sync dependencies and update lockfile:
```bash
uv sync
```

2. Build package distributions:
```bash
uv build
```

This will create source and wheel distributions in the `dist/` directory.

3. Publish to PyPI:
```bash
uv publish
```

Note: You'll need to set PyPI credentials via environment variables or command flags:
- Token: `--token` or `UV_PUBLISH_TOKEN`
- Or username/password: `--username`/`UV_PUBLISH_USERNAME` and `--password`/`UV_PUBLISH_PASSWORD`

### Debugging

Since MCP servers run over stdio, debugging can be challenging. For the best debugging
experience, we strongly recommend using the [MCP Inspector](https://github.com/modelcontextprotocol/inspector).


You can launch the MCP Inspector via [`npm`](https://docs.npmjs.com/downloading-and-installing-node-js-and-npm) with this command:

```bash
npx @modelcontextprotocol/inspector uv --directory /Users/user/generative_ai/model_context_protocol/s3-mcp-server run s3-mcp-server
```


Upon launching, the Inspector will display a URL that you can access in your browser to begin debugging.


## Security

See [CONTRIBUTING](CONTRIBUTING.md#security-issue-notifications) for more information.

## License

This library is licensed under the MIT-0 License. See the LICENSE file.