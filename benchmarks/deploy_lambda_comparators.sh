#!/usr/bin/env bash
# Deploy configuration-matched Lambda functions for the ZPQ/Polars/DuckDB
# benchmark. The functions are deliberately dedicated: never overwrite a
# general-purpose Lambda while collecting a release figure.
set -euo pipefail

cd "$(dirname "$0")/.."

ENV_FILE="${ENV_FILE:-.env}"
[[ -r "$ENV_FILE" ]] || { echo "missing readable ENV_FILE=$ENV_FILE" >&2; exit 2; }
set -a; source "$ENV_FILE"; set +a

REGION="${BENCH_REGION:-${AWS_REGION:-us-west-2}}"
MEMORY_MB="${BENCH_MEMORY_MB:-3008}"
TIMEOUT_S="${BENCH_TIMEOUT_S:-120}"
REPOSITORY="${BENCH_ECR_REPOSITORY:-zpq-lambda-comparators}"
ZPQ_FUNCTION="${ZPQ_BENCH_FUNCTION:-zpq-032-zig-x86}"
POLARS_FUNCTION="${POLARS_BENCH_FUNCTION:-zpq-032-polars-x86}"
DUCKDB_FUNCTION="${DUCKDB_BENCH_FUNCTION:-zpq-032-duckdb-x86}"

for fn in "$ZPQ_FUNCTION" "$POLARS_FUNCTION" "$DUCKDB_FUNCTION"; do
  [[ "$fn" == zpq-032-* ]] || {
    echo "refusing non-dedicated benchmark function name: $fn" >&2
    exit 2
  }
done

ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
REGISTRY="${ACCOUNT_ID}.dkr.ecr.${REGION}.amazonaws.com"
IMAGE_REPOSITORY="${REGISTRY}/${REPOSITORY}"
ROLE_ARN="${LAMBDA_BENCH_ROLE_ARN:-}"
if [[ -z "$ROLE_ARN" ]]; then
  ROLE_ARN=$(aws lambda get-function-configuration --function-name zpq-lambda-bench --region "$REGION" \
    --query Role --output text)
fi

ensure_repository() {
  aws ecr describe-repositories --repository-names "$REPOSITORY" --region "$REGION" >/dev/null 2>&1 || \
    aws ecr create-repository --repository-name "$REPOSITORY" --image-scanning-configuration scanOnPush=true \
      --region "$REGION" >/dev/null
}

deploy_zip() {
  local function_name="$1" zip_path="$2"
  if aws lambda get-function --function-name "$function_name" --region "$REGION" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$function_name" --zip-file "fileb://$zip_path" \
      --region "$REGION" >/dev/null
    aws lambda wait function-updated-v2 --function-name "$function_name" --region "$REGION"
    aws lambda update-function-configuration --function-name "$function_name" --memory-size "$MEMORY_MB" \
      --timeout "$TIMEOUT_S" --region "$REGION" >/dev/null
    aws lambda wait function-updated-v2 --function-name "$function_name" --region "$REGION"
  else
    aws lambda create-function --function-name "$function_name" --runtime provided.al2023 --handler bootstrap \
      --role "$ROLE_ARN" --zip-file "fileb://$zip_path" --memory-size "$MEMORY_MB" --timeout "$TIMEOUT_S" \
      --architectures x86_64 --region "$REGION" >/dev/null
    aws lambda wait function-active-v2 --function-name "$function_name" --region "$REGION"
  fi
}

deploy_image() {
  local function_name="$1" image_uri="$2"
  if aws lambda get-function --function-name "$function_name" --region "$REGION" >/dev/null 2>&1; then
    aws lambda update-function-code --function-name "$function_name" --image-uri "$image_uri" --region "$REGION" \
      >/dev/null
    aws lambda wait function-updated-v2 --function-name "$function_name" --region "$REGION"
    aws lambda update-function-configuration --function-name "$function_name" --memory-size "$MEMORY_MB" \
      --timeout "$TIMEOUT_S" --region "$REGION" >/dev/null
    aws lambda wait function-updated-v2 --function-name "$function_name" --region "$REGION"
  else
    aws lambda create-function --function-name "$function_name" --package-type Image --code ImageUri="$image_uri" \
      --role "$ROLE_ARN" --memory-size "$MEMORY_MB" --timeout "$TIMEOUT_S" --architectures x86_64 \
      --region "$REGION" >/dev/null
    aws lambda wait function-active-v2 --function-name "$function_name" --region "$REGION"
  fi
}

ensure_repository
aws ecr get-login-password --region "$REGION" | docker login --username AWS --password-stdin "$REGISTRY" >/dev/null

for engine in polars duckdb; do
  image="${IMAGE_REPOSITORY}:${engine}-current"
  docker build --platform linux/amd64 --build-arg ENGINE="$engine" -t "$image" benchmarks/python_baseline
  docker push "$image"
done

./tools/serverless/aws.sh build x86_64 >/dev/null
deploy_zip "$ZPQ_FUNCTION" zig-out/lambda/zpq-lambda-x86_64.zip
deploy_image "$POLARS_FUNCTION" "${IMAGE_REPOSITORY}:polars-current"
deploy_image "$DUCKDB_FUNCTION" "${IMAGE_REPOSITORY}:duckdb-current"

aws lambda get-function-configuration --function-name "$ZPQ_FUNCTION" --region "$REGION" \
  --query '[FunctionName,PackageType,Runtime,Architectures[0],MemorySize,Timeout,CodeSha256]' --output table
aws lambda get-function-configuration --function-name "$POLARS_FUNCTION" --region "$REGION" \
  --query '[FunctionName,PackageType,Runtime,Architectures[0],MemorySize,Timeout,CodeSha256]' --output table
aws lambda get-function-configuration --function-name "$DUCKDB_FUNCTION" --region "$REGION" \
  --query '[FunctionName,PackageType,Runtime,Architectures[0],MemorySize,Timeout,CodeSha256]' --output table
