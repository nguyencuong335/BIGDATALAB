#!/usr/bin/env bash
set -e

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_DIR"

echo "[1] Kiem tra input Bai 1..."
hdfs dfs -ls /user/cuong/bigdatalab/lab2/bai1/output

echo "[2] Xoa output cu neu da ton tai..."
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai4/output/top5_positive_words_by_category || true
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai4/output/top5_negative_words_by_category || true

echo "[3] Chay Pig script..."
pig -x mapreduce scripts/bai4.pig

echo "[4] Top 5 tu tich cuc theo category:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai4/output/top5_positive_words_by_category/part-* | head -50

echo "[5] Top 5 tu tieu cuc theo category:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai4/output/top5_negative_words_by_category/part-* | head -50