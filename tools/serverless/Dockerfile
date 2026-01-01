# Lambda container for zpq
#
# Build for ARM64 (Graviton):
#   docker build --platform linux/arm64 -t zpq-lambda .
#
# Build for x86_64:
#   docker build --platform linux/amd64 -t zpq-lambda .

FROM public.ecr.aws/lambda/provided:al2023-arm64

# Copy pre-built zpq binary as bootstrap
# Build with: ./tools/serverless/aws.sh build arm64
COPY zig-out/bin/zpq ${LAMBDA_RUNTIME_DIR}/bootstrap

RUN chmod +x ${LAMBDA_RUNTIME_DIR}/bootstrap

CMD ["handler"]
