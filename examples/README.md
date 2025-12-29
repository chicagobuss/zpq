# Examples

Real-world examples showing how to use zpq as a tool.

## Shell Examples

### Basic scanning

```bash
# Scan a local file
zpq scan data/sales.parquet

# Scan from S3
export AWS_ACCESS_KEY_ID=...
export AWS_SECRET_ACCESS_KEY=...
export AWS_REGION=us-west-2
zpq scan s3://my-bucket/data.parquet

# Inspect schema
zpq inspect data/sales.parquet
```

### Pipeline with jq

```bash
# Extract first column as JSON, process with jq
zpq scan data/users.parquet --format json | jq '.[] | select(.age > 30)'
```

## Python Integration

### Using PyArrow alongside zpq

```python
#!/usr/bin/env python3
"""Compare zpq and PyArrow performance on the same file."""
import subprocess
import time
import pyarrow.parquet as pq

path = "data/large.parquet"

# zpq timing
start = time.time()
result = subprocess.run(["zpq", "scan", path], capture_output=True)
zpq_time = time.time() - start

# PyArrow timing  
start = time.time()
table = pq.read_table(path)
_ = table.to_pandas()
pyarrow_time = time.time() - start

print(f"zpq:     {zpq_time:.3f}s")
print(f"PyArrow: {pyarrow_time:.3f}s")
print(f"Speedup: {pyarrow_time/zpq_time:.1f}x")
```

### S3 with boto3 pre-signed URLs

```python
#!/usr/bin/env python3
"""Use zpq with S3 pre-signed URLs for temporary access."""
import boto3
import subprocess

s3 = boto3.client('s3')
url = s3.generate_presigned_url(
    'get_object',
    Params={'Bucket': 'my-bucket', 'Key': 'data.parquet'},
    ExpiresIn=3600
)

# Pass pre-signed URL to zpq
result = subprocess.run(
    ["zpq", "scan", "--presigned-url", url],
    capture_output=True, text=True
)
print(result.stdout)
```

## Lambda Deployment

See `tools/lambda/` for AWS Lambda deployment examples.
