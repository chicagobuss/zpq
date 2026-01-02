# Lambda container for zpq benchmarks
# Uses released binary from GitHub/R2

FROM public.ecr.aws/lambda/provided:al2023-arm64

# Copy pre-downloaded zpq binary
COPY zpq ${LAMBDA_RUNTIME_DIR}/bootstrap
RUN chmod +x ${LAMBDA_RUNTIME_DIR}/bootstrap

CMD ["handler"]
