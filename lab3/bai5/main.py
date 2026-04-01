from pathlib import Path
from pyspark.sql import SparkSession


def parse_user(line):
    # users.txt: UserID,Gender,Age,Occupation,ZipCode
    parts = line.strip().split(",")
    user_id = int(parts[0])
    occupation_id = int(parts[3])
    return user_id, occupation_id


def parse_occupation(line):
    # occupation.txt: ID,Occupation
    parts = line.strip().split(",", 1)
    occupation_id = int(parts[0])
    occupation_name = parts[1]
    return occupation_id, occupation_name


def parse_rating(line):
    # ratings_x.txt: UserID,MovieID,Rating,Timestamp
    parts = line.strip().split(",")
    user_id = int(parts[0])
    rating = float(parts[2])
    return user_id, rating


if __name__ == "__main__":
    base_dir = Path(__file__).resolve().parents[1]
    data_dir = base_dir / "data"

    users_file = f"file://{(data_dir / 'users.txt').resolve()}"
    occupation_file = f"file://{(data_dir / 'occupation.txt').resolve()}"
    ratings_1_file = f"file://{(data_dir / 'ratings_1.txt').resolve()}"
    ratings_2_file = f"file://{(data_dir / 'ratings_2.txt').resolve()}"

    spark = (
        SparkSession.builder
        .appName("Lab3-Bai5")
        .master("local[*]")
        .config("spark.hadoop.fs.defaultFS", "file:///")
        .getOrCreate()
    )

    sc = spark.sparkContext
    sc.setLogLevel("ERROR")

    # ===== Bước 1: users.txt -> (UserID, OccupationID) =====
    users_rdd = sc.textFile(users_file).map(parse_user)

    # ===== Bước 2: occupation.txt -> (OccupationID, OccupationName) =====
    occupations_rdd = sc.textFile(occupation_file).map(parse_occupation)

    # ===== Bước 3: ratings -> (UserID, Rating) =====
    ratings_rdd = (
        sc.textFile(ratings_1_file)
        .union(sc.textFile(ratings_2_file))
        .map(parse_rating)
    )

    # ===== Bước 4: Join ratings với users =====
    # Kết quả: (UserID, (Rating, OccupationID))
    ratings_with_occupation_rdd = ratings_rdd.join(users_rdd)

    # ===== Bước 5: Đổi key thành OccupationID =====
    # Kết quả: (OccupationID, (Rating, 1))
    occupation_rating_rdd = ratings_with_occupation_rdd.map(
        lambda x: (x[1][1], (x[1][0], 1))
    )

    # ===== Bước 6: Tính tổng điểm và số lượt rating =====
    # Kết quả: (OccupationID, (sum_rating, count_rating))
    occupation_stats_rdd = occupation_rating_rdd.reduceByKey(
        lambda a, b: (a[0] + b[0], a[1] + b[1])
    )

    # ===== Bước 7: Tính điểm trung bình =====
    # Kết quả: (OccupationID, (avg_rating, count_rating))
    occupation_avg_rdd = occupation_stats_rdd.mapValues(
        lambda x: (x[0] / x[1], x[1])
    )

    # ===== Bước 8: Join với occupation name =====
    # Kết quả: (OccupationID, ((avg_rating, count_rating), OccupationName))
    result_rdd = occupation_avg_rdd.join(occupations_rdd).map(
        lambda x: (x[1][1], x[1][0][0], x[1][0][1])
    )
    # (OccupationName, avg_rating, count_rating)

    result_rdd = result_rdd.sortBy(lambda x: (-x[1], -x[2], x[0]))

    print("=== ĐIỂM TRUNG BÌNH VÀ TỔNG SỐ LƯỢT ĐÁNH GIÁ THEO NGHỀ NGHIỆP ===")
    for occupation_name, avg_rating, count_rating in result_rdd.collect():
        print(
            f"Occupation: {occupation_name} | Avg rating: {avg_rating:.4f} | Count: {count_rating}"
        )

    spark.stop()