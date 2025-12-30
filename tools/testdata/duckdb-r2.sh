#!/bin/bash
# Query Parquet files in R2 with DuckDB
#
# Usage:
#   ./tools/testdata/duckdb-r2.sh                    # Interactive mode
#   ./tools/testdata/duckdb-r2.sh "SELECT * FROM 's3://zpq/testdata/benchmark/benchmark_1mb.parquet' LIMIT 5"
#   ./tools/testdata/duckdb-r2.sh benchmark_1mb      # Shorthand for benchmark files

set -e

cd "$(dirname "$0")/../.."

if [ ! -f .env ]; then
    echo "Error: .env file not found"
    exit 1
fi

source .env

: "${R2_ACCOUNT_ID:?R2_ACCOUNT_ID not set}"
: "${R2_ACCESS_KEY_ID:?R2_ACCESS_KEY_ID not set}"
: "${R2_SECRET_ACCESS_KEY:?R2_SECRET_ACCESS_KEY not set}"

# DuckDB initialization SQL
INIT_SQL="
INSTALL httpfs; LOAD httpfs;
SET s3_endpoint='${R2_ACCOUNT_ID}.r2.cloudflarestorage.com';
SET s3_access_key_id='${R2_ACCESS_KEY_ID}';
SET s3_secret_access_key='${R2_SECRET_ACCESS_KEY}';
SET s3_region='auto';
SET s3_url_style='path';
"

# Helper function to expand shorthand paths
expand_path() {
    local query="$1"
    # Expand benchmark_Xmb -> full S3 path
    echo "$query" | sed -E "s|'benchmark_([0-9]+mb[^']*)'|'s3://zpq/testdata/benchmark/benchmark_\1.parquet'|g"
}

if [ $# -eq 0 ]; then
    # Interactive mode
    echo "DuckDB with R2 connection (s3://zpq/testdata/)"
    echo "Example: SELECT * FROM 's3://zpq/testdata/benchmark/benchmark_1mb.parquet' LIMIT 5;"
    echo ""
    duckdb -cmd "$INIT_SQL"
else
    # Query mode
    query=$(expand_path "$1")
    duckdb -c "$INIT_SQL $query"
fi
