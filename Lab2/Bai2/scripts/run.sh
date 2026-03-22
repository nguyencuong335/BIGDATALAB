#!/usr/bin/env bash
set -e

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_DIR"

echo "[1] Xoa output cu neu da ton tai..."
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai2/output/top5_words || true
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai2/output/category_count || true
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai2/output/aspect_count || true

echo "[2] Chay Pig script..."
pig -x mapreduce scripts/bai2.pig

echo "[3] Top 5 words:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai2/output/top5_words/part-* 

echo "[4] Category count:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai2/output/category_count/part-* | head -20

echo "[5] Aspect count:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai2/output/aspect_count/part-* | head -20