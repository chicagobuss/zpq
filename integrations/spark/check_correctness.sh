#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
OUT_DIR="${ZPQ_SPARK_OUT_DIR:-zig-out/spark}"
JDK_DIR="${ZPQ_SPARK_JDK_DIR:-$OUT_DIR/jdk-25}"
SPARK_DIR="${ZPQ_SPARK_HOME:-$OUT_DIR/spark-4.2.1}"
JAR_PATH="$OUT_DIR/zpq-spark.jar"
LIBZPQ_PATH="${ZPQ_LIB:-$REPO_ROOT/zig-out/lib/libzpq.so}"
CLICKBENCH_DIR="${1:-$OUT_DIR/data/clickbench}"
DECDATE_DIR="${2:-$OUT_DIR/data/decimal_dates}"

export JAVA_HOME="$JDK_DIR"
export SPARK_HOME="$SPARK_DIR"
export ZPQ_LIB="$LIBZPQ_PATH"
export TMPDIR="$OUT_DIR/tmp"
mkdir -p "$TMPDIR"

"$SCRIPT_DIR/build.sh"

"$SPARK_DIR/bin/spark-submit" \
  --master "local[16]" \
  --driver-memory 8g \
  --conf "spark.driver.extraJavaOptions=--enable-native-access=ALL-UNNAMED -Djava.io.tmpdir=$TMPDIR" \
  --conf "spark.executor.extraJavaOptions=--enable-native-access=ALL-UNNAMED -Djava.io.tmpdir=$TMPDIR" \
  --conf "spark.ui.enabled=false" \
  --conf "spark.sql.parquet.binaryAsString=true" \
  --class dev.zpq.spark.ZpqCorrectnessSuite \
  "$JAR_PATH" \
  "$CLICKBENCH_DIR" \
  "$DECDATE_DIR"
