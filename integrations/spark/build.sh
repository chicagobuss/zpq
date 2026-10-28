#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
OUT_DIR="${ZPQ_SPARK_OUT_DIR:-$HOME/code/zpq-out/spark}"
JDK_HOME="${JAVA_HOME:-$OUT_DIR/jdk-25}"
SPARK_HOME="${SPARK_HOME:-$OUT_DIR/spark-4.2.1}"
CLASSES_DIR="$OUT_DIR/build/classes"
JAR_PATH="${ZPQ_SPARK_JAR:-$OUT_DIR/zpq-spark.jar}"

rm -rf "$CLASSES_DIR"
mkdir -p "$CLASSES_DIR" "$OUT_DIR/tmp"

cp -r "$SCRIPT_DIR/src/main/resources/"* "$CLASSES_DIR/"

find "$SCRIPT_DIR/src/main/java" -name "*.java" > "$OUT_DIR/build/sources.txt"

TMPDIR="$OUT_DIR/tmp" "$JDK_HOME/bin/javac" \
  --release 25 \
  -cp "$SPARK_HOME/jars/*" \
  -d "$CLASSES_DIR" \
  @"$OUT_DIR/build/sources.txt"

TMPDIR="$OUT_DIR/tmp" "$JDK_HOME/bin/jar" \
  --create \
  --file "$JAR_PATH" \
  -C "$CLASSES_DIR" .

echo "Built $JAR_PATH"
