from pathlib import Path
from pyspark.sql import SparkSession


def parse_movie(line):
    # movies.txt: MovieID,Title,Genres
    parts = line.strip().split(",", 2)
    movie_id = int(parts[0])
    genres = parts[2].split("|")
    return movie_id, genres


def parse_rating(line):
    # ratings_x.txt: UserID,MovieID,Rating,Timestamp
    parts = line.strip().split(",")
    movie_id = int(parts[1])
    rating = float(parts[2])
    return movie_id, rating


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"

    movies_file = f"file://{(data_dir / 'movies.txt').resolve()}"
    ratings_1_file = f"file://{(data_dir / 'ratings_1.txt').resolve()}"
    ratings_2_file = f"file://{(data_dir / 'ratings_2.txt').resolve()}"

    spark = (
        SparkSession.builder
        .appName("Lab3-Bai2")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # ===== Bước 1: Đọc movies.txt -> (MovieID, [Genres]) =====
    movies_rdd = sc.textFile(movies_file).map(parse_movie)

    # ===== Bước 2: Đọc 2 file ratings -> (MovieID, Rating) =====
    ratings_rdd = (
        sc.textFile(ratings_1_file)
        .union(sc.textFile(ratings_2_file))
        .map(parse_rating)
    )

    # ===== Bước 3: Join ratings với movies =====
    # Kết quả: (MovieID, (Rating, [Genres]))
    joined_rdd = ratings_rdd.join(movies_rdd)

    # ===== Bước 4: Bung mỗi phim thành nhiều genre =====
    # Ví dụ: (1025, (4.5, ["Action", "Sci-Fi"]))
    # -> ("Action", (4.5, 1)), ("Sci-Fi", (4.5, 1))
    genre_rating_rdd = joined_rdd.flatMap(
        lambda x: [(genre, (x[1][0], 1)) for genre in x[1][1]]
    )

    # ===== Bước 5: Tính tổng điểm và số lượt đánh giá cho từng genre =====
    genre_stats_rdd = genre_rating_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # ===== Bước 6: Tính điểm trung bình =====
    genre_avg_rdd = genre_stats_rdd.mapValues(
        lambda x: (x[0] / x[1], x[1])
    )
    # Kết quả: (Genre, (avg_rating, count_rating))

    # ===== Sắp xếp giảm dần theo avg_rating =====
    result_rdd = genre_avg_rdd.sortBy(lambda x: (-x[1][0], -x[1][1], x[0]))

    print("=== ĐIỂM TRUNG BÌNH THEO THỂ LOẠI ===")
    for genre, (avg_rating, count_rating) in result_rdd.collect():
        print(f"Genre: {genre} | Avg rating: {avg_rating:.4f} | Count: {count_rating}")

    spark.stop()