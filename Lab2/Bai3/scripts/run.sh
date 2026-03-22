#!/usr/bin/env bash
set -e

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_DIR"

echo "[1] Kiem tra input Bai 1..."
hdfs dfs -ls /user/cuong/bigdatalab/lab2/bai1/output

echo "[2] Xoa output cu neu da ton tai..."
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai3/output/top_negative_aspect || true
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai3/output/top_positive_aspect || true

echo "[3] Chay Pig script..."
pig -x mapreduce scripts/bai3.pig

echo "[4] Ket qua aspect negative nhieu nhat:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai3/output/top_negative_aspect/part-*

echo "[5] Ket qua aspect positive nhieu nhat:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai3/output/top_positive_aspect/part-*