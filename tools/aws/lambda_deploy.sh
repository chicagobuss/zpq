#!/bin/bash
set -e

# Usage: ./tools/aws/lambda_deploy.sh [example-name]
# Example: ./tools/aws/lambda_deploy.sh 01-minimal

# Load environment variables if .env exists
if [ -f .env ]; then
    # Use -a to export all variables
    set -a
    source .env
    set +a
fi

EXAMPLE=${1:-"01-minimal"}
FUNCTION_NAME=${LAMBDA_FUNCTION_NAME:-"zpq-lambda-bench"}
REGION=${AWS_REGION:-"us-west-2"}

echo "--- Deploying Lambda: lambda-$EXAMPLE to $FUNCTION_NAME in $REGION ---"

# 1. Build the binary for Lambda (ARM64 Linux)
echo "Building..."
zig build "example-lambda-$EXAMPLE" -Dexamples -Dtarget=aarch64-linux -Doptimize=ReleaseSmall

# 2. Package into a ZIP
echo "Packaging..."
cd "zig-out/lambda/lambda-$EXAMPLE"
zip -q -j lambda.zip bootstrap
cd - > /dev/null

# 3. Create or Update Lambda Function
echo "Deploying to AWS..."
if aws lambda get-function --function-name "$FUNCTION_NAME" --region "$REGION" > /dev/null 2>&1; then
    echo "Updating existing function: $FUNCTION_NAME"
    aws lambda update-function-code \
        --function-name "$FUNCTION_NAME" \
        --zip-file "fileb://zig-out/lambda/lambda-$EXAMPLE/lambda.zip" \
        --region "$REGION" \
        --no-cli-pager > /dev/null
else
    echo "Creating new function: $FUNCTION_NAME"
    if [ -z "$LAMBDA_ROLE_ARN" ]; then
        echo "Error: LAMBDA_ROLE_ARN not set. Cannot create function."
        exit 1
    fi
    aws lambda create-function \
        --function-name "$FUNCTION_NAME" \
        --runtime provided.al2023 \
        --role "$LAMBDA_ROLE_ARN" \
        --handler bootstrap \
        --zip-file "fileb://zig-out/lambda/lambda-$EXAMPLE/lambda.zip" \
        --architectures arm64 \
        --region "$REGION" \
        --no-cli-pager > /dev/null
fi

echo "Waiting for update to complete..."
aws lambda wait function-updated \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION"

# 4. Invoke and show results
echo "Invoking..."
aws lambda invoke \
    --function-name "$FUNCTION_NAME" \
    --region "$REGION" \
    --payload '{}' \
    --cli-binary-format raw-in-base64-out \
    --log-type Tail \
    response.json > result.json

echo "Response:"
cat response.json
echo ""

echo "--- CloudWatch Statistics ---"
cat result.json | jq -r '.LogResult' | base64 --decode | grep "REPORT"
echo "----------------------------"

# 5. Clean up
rm response.json result.json
echo "Done."

