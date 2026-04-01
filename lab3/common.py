from pathlib import Path
from datetime import datetime, timezone
from pyspark.sql import SparkSession

ROOT_DIR = Path(__file__).resolve().parent
DATA_DIR = ROOT_DIR / "data"

MOVIES_FILE = DATA_DIR / "movies.txt"
RATINGS_1_FILE = DATA_DIR / "ratings_1.txt"
RATINGS_2_FILE = DATA_DIR / "ratings_2.txt"
USERS_FILE = DATA_DIR / "users.txt"
OCCUPATION_FILE = DATA_DIR / "occupation.txt"


def create_spark(app_name: str) -> SparkSession:
    spark = (
        SparkSession.builder
        .appName(app_name)
        .master("local[*]")
        .config("spark.ui.showConsoleProgress", "true")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def read_ratings_lines(sc):
    return sc.textFile(str(RATINGS_1_FILE)).union(sc.textFile(str(RATINGS_2_FILE)))


def parse_movie(line: str):
    movie_id, title, genres = line.strip().split(",", 2)
    return int(movie_id), title, genres.split("|")


def parse_rating(line: str):
    user_id, movie_id, rating, timestamp = line.strip().split(",")
    return int(user_id), int(movie_id), float(rating), int(timestamp)


def parse_user(line: str):
    user_id, gender, age, occupation, zip_code = line.strip().split(",")
    return int(user_id), gender, int(age), int(occupation), zip_code


def parse_occupation(line: str):
    occupation_id, occupation_name = line.strip().split(",", 1)
    return int(occupation_id), occupation_name


def age_group(age: int) -> str:
    if age < 18:
        return "Under 18"
    if age <= 25:
        return "18-25"
    if age <= 35:
        return "26-35"
    if age <= 45:
        return "36-45"
    if age <= 55:
        return "46-55"
    return "56+"


def to_year(timestamp: int) -> int:
    return datetime.fromtimestamp(timestamp, tz=timezone.utc).year