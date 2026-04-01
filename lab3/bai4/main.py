from pathlib import Path
from pyspark.sql import SparkSession


def parse_movie(line):
    # movies.txt: MovieID,Title,Genres
    parts = line.strip().split(",", 2)
    movie_id = int(parts[0])
    title = parts[1]
    return movie_id, title


def parse_rating(line):
    # ratings_x.txt: UserID,MovieID,Rating,Timestamp
    parts = line.strip().split(",")
    user_id = int(parts[0])
    movie_id = int(parts[1])
    rating = float(parts[2])
    return user_id, (movie_id, rating)


def parse_user(line):
    # users.txt: UserID,Gender,Age,Occupation,ZipCode
    parts = line.strip().split(",")
    user_id = int(parts[0])
    age = int(parts[2])
    return user_id, age


def get_age_group(age):
    if age < 18:
        return "Under 18"
    elif age <= 25:
        return "18-25"
    elif age <= 35:
        return "26-35"
    elif age <= 45:
        return "36-45"
    elif age <= 55:
        return "46-55"
    else:
        return "56+"


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"

    movies_file = f"file://{(data_dir / 'movies.txt').resolve()}"
    ratings_1_file = f"file://{(data_dir / 'ratings_1.txt').resolve()}"
    ratings_2_file = f"file://{(data_dir / 'ratings_2.txt').resolve()}"
    users_file = f"file://{(data_dir / 'users.txt').resolve()}"

    spark = (
        SparkSession.builder
        .appName("Lab3-Bai4")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # ===== Bước 1: Đọc users.txt -> (UserID, AgeGroup) =====
    users_rdd = (
        sc.textFile(users_file)
        .map(parse_user)
        .map(lambda x: (x[0], get_age_group(x[1])))
    )

    # ===== Bước 2: Đọc ratings -> (UserID, (MovieID, Rating)) =====
    ratings_rdd = (
        sc.textFile(ratings_1_file)
        .union(sc.textFile(ratings_2_file))
        .map(parse_rating)
    )

    # ===== Bước 3: Join ratings với users =====
    # Kết quả: (UserID, ((MovieID, Rating), AgeGroup))
    ratings_with_age_rdd = ratings_rdd.join(users_rdd)

    # ===== Bước 4: Đổi key thành (MovieID, AgeGroup) =====
    # Kết quả: ((MovieID, AgeGroup), (Rating, 1))
    movie_age_rating_rdd = ratings_with_age_rdd.map(
        lambda x: ((x[1][0][0], x[1][1]), (x[1][0][1], 1))
    )

    # ===== Bước 5: Tính tổng điểm và số lượt rating =====
    # Kết quả: ((MovieID, AgeGroup), (sum_rating, count_rating))
    movie_age_stats_rdd = movie_age_rating_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # ===== Bước 6: Tính điểm trung bình =====
    # Kết quả: ((MovieID, AgeGroup), (avg_rating, count_rating))
    movie_age_avg_rdd = movie_age_stats_rdd.mapValues(
        lambda x: (x[0] / x[1], x[1])
    )

    # ===== Bước 7: Đọc movies.txt -> (MovieID, Title) =====
    movies_rdd = sc.textFile(movies_file).map(parse_movie)

    # Đổi về dạng: (MovieID, (AgeGroup, avg_rating, count_rating))
    temp_rdd = movie_age_avg_rdd.map(
        lambda x: (x[0][0], (x[0][1], x[1][0], x[1][1]))
    )

    # ===== Bước 8: Join với movie title =====
    result_rdd = temp_rdd.join(movies_rdd).map(
        lambda x: (x[1][1], x[1][0][0], x[1][0][1], x[1][0][2])
    )
    # (Title, AgeGroup, avg_rating, count_rating)

    result_rdd = result_rdd.sortBy(lambda x: (x[0], x[1]))

    print("=== ĐIỂM TRUNG BÌNH CỦA MỖI PHIM THEO NHÓM TUỔI ===")
    for title, age_group, avg_rating, count_rating in result_rdd.collect():
        print(
            f"Title: {title} | Age group: {age_group} | Avg rating: {avg_rating:.4f} | Count: {count_rating}"
        )

    spark.stop()