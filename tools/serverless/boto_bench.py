import boto3
import time
import json
import os
import concurrent.futures

s3 = boto3.client('s3')

def lambda_handler(event, context):
    src_path = event.get('file')
    dst_path = event.get('output')
    
    # Parse S3 URLs
    src_bucket = src_path.split('/')[2]
    src_key = '/'.join(src_path.split('/')[3:])
    
    dst_bucket = dst_path.split('/')[2]
    dst_key = '/'.join(dst_path.split('/')[3:])
    
    print(f"Reading from {src_bucket}/{src_key}")
    print(f"Writing to {dst_bucket}/{dst_key}")
    
    start = time.time()
    
    # Use s3.get_object and s3.upload_fileobj for streaming copy
    # This forces data to pass through the Lambda (unlike copy_object)
    
    # 1. Get object stream
    response = s3.get_object(Bucket=src_bucket, Key=src_key)
    body = response['Body']
    
    # 2. Upload stream
    # Boto3 upload_fileobj automatically handles multipart uploads and threading
    s3.upload_fileobj(body, dst_bucket, dst_key)
    
    end = time.time()
    duration = end - start
    size_mb = response['ContentLength'] / (1024 * 1024)
    throughput = size_mb / duration
    
    print(f"Done in {duration:.2f}s. Throughput: {throughput:.2f} MB/s")
    
    return {
        "duration_s": duration,
        "throughput_mb_s": throughput,
        "size_bytes": response['ContentLength']
    }
