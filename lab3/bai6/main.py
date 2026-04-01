from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession


def parse_rating(line):
    # ratings_x.txt: UserID,MovieID,Rating,Timestamp
    parts = line.strip().split(",")
    user_id = int(parts[0])
    movie_id = int(parts[1])
    rating = float(parts[2])
    timestamp = int(parts[3])
    return user_id, movie_id, rating, timestamp


def get_year_from_timestamp(timestamp):
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).year


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"

    ratings_1_file = f"file://{(data_dir / 'ratings_1.txt').resolve()}"
    ratings_2_file = f"file://{(data_dir / 'ratings_2.txt').resolve()}"

    spark = (
        SparkSession.builder
        .appName("Lab3-Bai6")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # ===== Bước 1: Đọc 2 file ratings =====
    ratings_rdd = (
        sc.textFile(ratings_1_file)
        .union(sc.textFile(ratings_2_file))
        .map(parse_rating)
    )

    # ===== Bước 2: Đổi timestamp -> year, tạo (year, (rating, 1)) =====
    year_rating_rdd = ratings_rdd.map(
        lambda x: (get_year_from_timestamp(x[3]), (x[2], 1))
    )

    # ===== Bước 3: Tính tổng điểm và số lượt đánh giá cho mỗi năm =====
    year_stats_rdd = year_rating_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # ===== Bước 4: Tính điểm trung bình =====
    year_avg_rdd = year_stats_rdd.mapValues(
        lambda x: (x[0] / x[1], x[1])
    )
    # Kết quả: (year, (avg_rating, count_rating))

    # ===== Bước 5: Sắp xếp theo năm tăng dần =====
    result_rdd = year_avg_rdd.sortByKey()

    print("=== TỔNG SỐ LƯỢT ĐÁNH GIÁ VÀ ĐIỂM TRUNG BÌNH THEO NĂM ===")
    for year, (avg_rating, count_rating) in result_rdd.collect():
        print(f"Year: {year} | Avg rating: {avg_rating:.4f} | Count: {count_rating}")

    spark.stop()