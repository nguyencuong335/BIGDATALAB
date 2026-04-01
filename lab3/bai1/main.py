from pathlib import Path
from pyspark.sql import SparkSession


def parse_movie(line):
    # movies.txt: MovieID,Title,Genres
    parts = line.strip().split(",", 2)
    return int(parts[0]), parts[1]


def parse_rating(line):
    # ratings_x.txt: UserID,MovieID,Rating,Timestamp
    parts = line.strip().split(",")
    return int(parts[1]), float(parts[2])   # (MovieID, Rating)


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"

    movies_file = f"file://{(data_dir / 'movies.txt').resolve()}"
    ratings_1_file = f"file://{(data_dir / 'ratings_1.txt').resolve()}"
    ratings_2_file = f"file://{(data_dir / 'ratings_2.txt').resolve()}"

    spark = (
        SparkSession.builder
        .appName("Lab3-Bai1")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # Bước 1: Đọc movies.txt -> (MovieID, Title)
    movies_rdd = sc.textFile(movies_file).map(parse_movie)

    # Bước 2: Đọc 2 file ratings, ghép lại
    ratings_rdd = (
        sc.textFile(ratings_1_file)
        .union(sc.textFile(ratings_2_file))
        .map(parse_rating)                  # (MovieID, Rating)
        .map(lambda x: (x[0], (x[1], 1)))   # (MovieID, (Rating, 1))
    )

    # Bước 3: Tính tổng điểm và số lượt đánh giá
    rating_stats_rdd = ratings_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # Bước 4: Tính điểm trung bình
    avg_rating_rdd = rating_stats_rdd.mapValues(
        lambda x: (x[0] / x[1], x[1])
    )

    # Bước 5: Join với movies để lấy tên phim
    joined_rdd = avg_rating_rdd.join(movies_rdd)

    result_rdd = joined_rdd.map(
        lambda x: (x[1][1], x[1][0][0], x[1][0][1])
    )

    print("=== TẤT CẢ PHIM ===")
    all_movies = result_rdd.sortBy(lambda x: (-x[1], -x[2], x[0]))
    for row in all_movies.collect():
        print(row)

    filtered_rdd = result_rdd.filter(lambda x: x[2] >= 50)

    print("\n=== PHIM CÓ ÍT NHẤT 50 LƯỢT ĐÁNH GIÁ ===")
    filtered_list = filtered_rdd.sortBy(lambda x: (-x[1], -x[2], x[0])).collect()

    if len(filtered_list) == 0:
        print("Không có phim nào đủ điều kiện >= 50 lượt đánh giá.")
    else:
        for row in filtered_list:
            print(row)

        best_movie = filtered_list[0]
        print("\n=== PHIM CÓ ĐIỂM TRUNG BÌNH CAO NHẤT (>=50 ratings) ===")
        print(best_movie)

    spark.stop()