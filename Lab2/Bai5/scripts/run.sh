#!/usr/bin/env bash
set -e

PROJECT_DIR=$(cd "$(dirname "$0")/.." && pwd)
cd "$PROJECT_DIR"

echo "[1] Kiem tra input Bai 1..."
hdfs dfs -ls /user/cuong/bigdatalab/lab2/bai1/output

echo "[2] Xoa output cu neu da ton tai..."
hdfs dfs -rm -r -f /user/cuong/bigdatalab/lab2/bai5/output/top5_related_words_by_category || true

echo "[3] Chay Pig script..."
pig -x mapreduce scripts/bai5.pig

echo "[4] Ket qua top 5 tu lien quan nhat theo category:"
hdfs dfs -cat /user/cuong/bigdatalab/lab2/bai5/output/top5_related_words_by_category/part-* | head -50