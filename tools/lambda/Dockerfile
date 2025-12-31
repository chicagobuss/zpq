# Lambda container for zpq
#
# Build for ARM64 (Graviton):
#   docker build --platform linux/arm64 -t zpq-lambda .
#
# Build for x86_64:
#   docker build --platform linux/amd64 -t zpq-lambda .

FROM public.ecr.aws/lambda/provided:al2023-arm64

# Copy pre-built bootstrap binary
# Build with: zig build example-lambda-04-scan-benchmark -Dexamples -Dtarget=aarch64-linux -Doptimize=ReleaseFast
COPY zig-out/lambda/lambda-04-scan-benchmark/bootstrap ${LAMBDA_RUNTIME_DIR}/bootstrap

RUN chmod +x ${LAMBDA_RUNTIME_DIR}/bootstrap

CMD ["handler"]
