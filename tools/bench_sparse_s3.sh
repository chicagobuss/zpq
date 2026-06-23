#!/usr/bin/env bash
set -euo pipefail

if [[ $# -lt 1 ]]; then
  echo "usage: $0 s3://bucket/key [aggregate]" >&2
  exit 2
fi

url="$1"
agg="${2:-count(*) AS n, sum(id) AS sum_id, sum(metric) AS sum_metric}"

if [[ -f .env ]]; then
  set -a
  # shellcheck disable=SC1091
  . ./.env >/dev/null 2>&1
  set +a
fi

if [[ -n "${R2_ACCESS_KEY_ID:-}" ]]; then
  export AWS_ACCESS_KEY_ID="$R2_ACCESS_KEY_ID"
  export AWS_SECRET_ACCESS_KEY="$R2_SECRET_ACCESS_KEY"
  export AWS_REGION="${R2_REGION:-auto}"
  export S3_ENDPOINT_URL="https://${R2_ENDPOINT:?R2_ENDPOINT missing}"
fi

/usr/bin/time -v ./zig-out/bin/zpq query "$url" --aggregate "$agg"
