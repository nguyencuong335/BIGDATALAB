#!/usr/bin/env bash

set -e

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_DIR"

echo "[1] Compile Java UDF..."
mkdir -p build/classes

"$JAVA_HOME/bin/javac" --release 11 -cp "$(hadoop classpath):$PIG_HOME/*:$PIG_HOME/lib/*" \
      -d build/classes \
      src/bigdata/CleanReview.java

echo "[2] Create jar..."
jar -cvf build/preprocess.jar -C build/classes .

echo "[3] Upload input files to HDFS..."
hdfs dfs -mkdir -p /user/cuong/bigdatalab/lab2/bai1/input
hdfs dfs -put -f data/hotel-review.csv /user/cuong/bigdatalab/lab2/bai1/input/
hdfs dfs -put -f data/stopwords.txt /user/cuong/bigdatalab/lab2/bai1/input/

echo "[4] Remove old output if exists..."
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai1/output || true

echo "[5] Run Pig script..."
pig -x mapreduce scripts/bai1.pig

echo "[6] Show output from HDFS..."
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai1/output/part-* | head