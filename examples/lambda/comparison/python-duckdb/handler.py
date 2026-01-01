"""
DuckDB-powered Lambda for Parquet filtering.
Apples-to-apples comparison with ZPQ.
"""
import json
import os
import time
import duckdb
import boto3

def handler(event, context):
    start = time.perf_counter()

    input_path = event.get('input_path')
    output_path = event.get('output_path')
    filter_column = event.get('filter_column')
    filter_value = event.get('filter_value')

    if not all([input_path, output_path, filter_column, filter_value]):
        return {'error': 'Missing required parameters'}

    # Parse S3 paths
    def parse_s3(path):
        path = path.replace('s3://', '')
        bucket, key = path.split('/', 1)
        return bucket, key

    in_bucket, in_key = parse_s3(input_path)
    out_bucket, out_key = parse_s3(output_path)

    # Configure DuckDB
    con = duckdb.connect(':memory:')
    con.execute("SET home_directory = '/tmp';")

    # Download file first via boto3 (more reliable in Lambda than httpfs)
    download_start = time.perf_counter()
    s3 = boto3.client('s3')
    local_input = '/tmp/input.parquet'
    s3.download_file(in_bucket, in_key, local_input)
    download_time = (time.perf_counter() - download_start) * 1000

    # Count input rows
    count_start = time.perf_counter()
    input_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{local_input}')").fetchone()[0]
    count_time = (time.perf_counter() - count_start) * 1000

    # Filter and write to local temp file
    filter_start = time.perf_counter()
    local_temp = '/tmp/filtered_output.parquet'

    # 25 columns for realistic cost analysis query (verified to exist in file)
    columns = """
        bill_payer_account_id,
        bill_payer_account_name,
        identity_line_item_id,
        identity_time_interval,
        line_item_availability_zone,
        line_item_blended_cost,
        line_item_blended_rate,
        line_item_currency_code,
        line_item_line_item_description,
        line_item_line_item_type,
        line_item_operation,
        line_item_product_code,
        line_item_resource_id,
        line_item_unblended_cost,
        line_item_unblended_rate,
        line_item_usage_account_id,
        line_item_usage_amount,
        line_item_usage_end_date,
        line_item_usage_start_date,
        line_item_usage_type,
        product_product_family,
        product_region_code,
        product_servicecode,
        pricing_public_on_demand_cost,
        pricing_public_on_demand_rate
    """

    # DuckDB filter + write with column projection
    con.execute(f"""
        COPY (
            SELECT {columns} FROM read_parquet('{local_input}')
            WHERE "{filter_column}" = '{filter_value}'
        ) TO '{local_temp}' (FORMAT PARQUET, COMPRESSION SNAPPY)
    """)
    filter_time = (time.perf_counter() - filter_start) * 1000

    # Get output row count
    output_rows = con.execute(f"SELECT COUNT(*) FROM read_parquet('{local_temp}')").fetchone()[0]

    # Upload to S3
    upload_start = time.perf_counter()
    file_size = os.path.getsize(local_temp)
    s3.upload_file(local_temp, out_bucket, out_key)
    upload_time = (time.perf_counter() - upload_start) * 1000

    total_time = (time.perf_counter() - start) * 1000

    # Cleanup
    os.remove(local_input)
    os.remove(local_temp)
    con.close()

    return {
        'status': 'success',
        'input_rows': input_rows,
        'output_rows': output_rows,
        'output_size_bytes': file_size,
        'download_time_ms': round(download_time, 2),
        'count_time_ms': round(count_time, 2),
        'filter_time_ms': round(filter_time, 2),
        'upload_time_ms': round(upload_time, 2),
        'total_time_ms': round(total_time, 2),
        'output_path': output_path
    }
