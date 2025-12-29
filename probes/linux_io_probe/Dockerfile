FROM debian:bookworm-slim
RUN apt-get update && apt-get install -y curl xz-utils jq ca-certificates
# Fetch latest master for aarch64 (since we are on Apple Silicon, Docker runs native aarch64)
RUN curl -s https://ziglang.org/download/index.json | jq -r '.master."aarch64-linux".tarball' | xargs curl -o zig.tar.xz
RUN mkdir -p /usr/local/zig
RUN tar -xf zig.tar.xz -C /usr/local/zig --strip-components=1
ENV PATH="/usr/local/zig:${PATH}"
WORKDIR /app
COPY main.zig .
RUN zig version
CMD ["zig", "run", "main.zig"]
