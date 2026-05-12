from __future__ import annotations

from datetime import datetime
from pathlib import Path
import os
import sys

from pyspark import SparkConf, SparkContext


BASE_DIR = Path(__file__).resolve().parent.parent
MOVIES_PATH = BASE_DIR / "movies(2).txt"
RATINGS_1_PATH = BASE_DIR / "ratings_1(1).txt"
RATINGS_2_PATH = BASE_DIR / "ratings_2(1).txt"
USERS_PATH = BASE_DIR / "users(1).txt"
OCCUPATIONS_PATH = BASE_DIR / "occupation.txt"
OUTPUT_DIR = BASE_DIR / "output_source"


def configure_environment() -> None:
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(encoding="utf-8")
    if hasattr(sys.stderr, "reconfigure"):
        sys.stderr.reconfigure(encoding="utf-8")


def validate_input_files() -> None:
    required_files = [
        MOVIES_PATH,
        RATINGS_1_PATH,
        RATINGS_2_PATH,
        USERS_PATH,
        OCCUPATIONS_PATH,
    ]
    missing_files = [str(path) for path in required_files if not path.exists()]
    if missing_files:
        raise FileNotFoundError(
            "Không tìm thấy file dữ liệu: " + ", ".join(missing_files)
        )


def create_spark_context() -> SparkContext:
    configure_environment()
    conf = (
        SparkConf()
        .setMaster("local[*]")
        .setAppName("TH3 Spark RDD Movie Ratings Script")
        .set("spark.driver.host", "127.0.0.1")
        .set("spark.driver.bindAddress", "127.0.0.1")
        .set("spark.executorEnv.PYSPARK_PYTHON", sys.executable)
    )
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    return sc


def spark_file(path: Path) -> str:
    return str(path.resolve()).replace("\\", "/")


def parse_movie(line: str) -> tuple[int, tuple[str, list[str]]]:
    movie_id_text, rest = line.strip().split(",", 1)
    title, genres_text = rest.rsplit(",", 1)
    genres = [genre.strip() for genre in genres_text.split("|") if genre.strip()]
    return int(movie_id_text), (title.strip(), genres)


def parse_rating(line: str) -> tuple[int, int, float, int]:
    user_id, movie_id, rating, timestamp = line.strip().split(",")
    return int(user_id), int(movie_id), float(rating), int(timestamp)


def parse_user(line: str) -> tuple[int, str, int, int, str]:
    user_id, gender, age, occupation_id, zip_code = line.strip().split(",")
    return int(user_id), gender, int(age), int(occupation_id), zip_code


def parse_occupation(line: str) -> tuple[int, str]:
    occupation_id, occupation = line.strip().split(",", 1)
    return int(occupation_id), occupation.strip()


def sum_rating_counts(
    left: tuple[float, int], right: tuple[float, int]
) -> tuple[float, int]:
    return left[0] + right[0], left[1] + right[1]


def average_with_count(value: tuple[float, int]) -> tuple[float, int]:
    total_rating, total_count = value
    return round(total_rating / total_count, 2), total_count


def age_group(age: int) -> str:
    if age < 18:
        return "<18"
    if age <= 24:
        return "18-24"
    if age <= 34:
        return "25-34"
    if age <= 44:
        return "35-44"
    if age <= 49:
        return "45-49"
    if age <= 55:
        return "50-55"
    return "56+"


def year_from_unix(timestamp: int) -> int:
    return datetime.utcfromtimestamp(timestamp).year


def format_table(
    title: str, rows: list[tuple], headers: list[str] | None = None
) -> str:
    lines = ["", title, "=" * len(title)]
    if headers:
        header_line = " | ".join(headers)
        lines.append(header_line)
        lines.append("-" * max(20, len(header_line)))
    if not rows:
        lines.append("Không có dữ liệu.")
    else:
        for row in rows:
            lines.append(" | ".join(str(value) for value in row))
    return "\n".join(lines)


def write_output(stem: str, content: str) -> None:
    OUTPUT_DIR.mkdir(exist_ok=True)
    (OUTPUT_DIR / f"{stem}.txt").write_text(content.strip() + "\n", encoding="utf-8")


def main() -> None:
    validate_input_files()
    sc = create_spark_context()
    try:
        print("Spark version:", sc.version)
        print("Base directory:", BASE_DIR)

        movies = sc.textFile(spark_file(MOVIES_PATH)).map(parse_movie).cache()
        ratings_1 = sc.textFile(spark_file(RATINGS_1_PATH)).map(parse_rating).cache()
        ratings_2 = sc.textFile(spark_file(RATINGS_2_PATH)).map(parse_rating).cache()
        ratings = ratings_1.union(ratings_2).cache()
        users = sc.textFile(spark_file(USERS_PATH)).map(parse_user).cache()
        occupations = sc.textFile(spark_file(OCCUPATIONS_PATH)).map(parse_occupation).cache()

        movie_titles = movies.mapValues(lambda value: value[0]).cache()
        movie_genres = movies.mapValues(lambda value: value[1]).cache()
        ratings_by_movie = ratings.map(lambda row: (row[1], (row[2], 1))).cache()
        ratings_by_user = ratings.map(lambda row: (row[0], (row[1], row[2], row[3]))).cache()
        users_by_id = users.map(lambda row: (row[0], row[1:])).cache()

        movies_count = movies.count()
        ratings_1_count = ratings_1.count()
        ratings_2_count = ratings_2.count()
        ratings_count = ratings.count()
        users_count = users.count()
        occupation_count = occupations.count()
        rating_movie_join_count = ratings.map(lambda row: (row[1], row)).join(movie_titles).count()
        rating_user_join_count = ratings_by_user.join(users_by_id).count()

        summary_lines = [
            f"Số phim: {movies_count}",
            f"Số ratings file 1: {ratings_1_count}",
            f"Số ratings file 2: {ratings_2_count}",
            f"Tổng số ratings: {ratings_count}",
            f"Số users: {users_count}",
            f"Số occupations: {occupation_count}",
            f"Ratings join được với movies: {rating_movie_join_count}",
            f"Ratings join được với users: {rating_user_join_count}",
        ]
        summary_text = "\n".join(summary_lines)
        print(summary_text)

        assert ratings_count == ratings_1_count + ratings_2_count == 184
        assert rating_movie_join_count == ratings_count
        assert rating_user_join_count == ratings_count

        movie_stats = (
            ratings_by_movie.reduceByKey(sum_rating_counts)
            .mapValues(average_with_count)
            .cache()
        )
        movie_stats_with_title = (
            movie_stats.join(movie_titles)
            .map(lambda item: (item[0], item[1][1], item[1][0][0], item[1][0][1]))
            .cache()
        )
        top_movies = movie_stats_with_title.sortBy(
            lambda row: (-row[2], -row[3], row[1])
        ).take(15)
        best_min_50 = (
            movie_stats_with_title.filter(lambda row: row[3] >= 50)
            .sortBy(lambda row: (-row[2], row[1]))
            .take(1)
        )
        best_min_5 = (
            movie_stats_with_title.filter(lambda row: row[3] >= 5)
            .sortBy(lambda row: (-row[2], row[1]))
            .take(1)
        )
        bai1_text = "\n".join(
            [
                format_table(
                    "Bài 1 - Điểm trung bình theo phim",
                    top_movies,
                    ["MovieID", "Title", "AvgRating", "TotalRatings"],
                ),
                format_table(
                    "Phim điểm trung bình cao nhất với tối thiểu 50 lượt đánh giá",
                    best_min_50,
                    ["MovieID", "Title", "AvgRating", "TotalRatings"],
                ),
                (
                    "Ghi chú: Không có phim nào đạt tối thiểu 50 lượt đánh giá trong bộ dữ liệu này."
                    if not best_min_50
                    else ""
                ),
                format_table(
                    "Kết quả minh họa với tối thiểu 5 lượt đánh giá",
                    best_min_5,
                    ["MovieID", "Title", "AvgRating", "TotalRatings"],
                ),
            ]
        ).strip()
        print(bai1_text)
        write_output("bai1", bai1_text)

        genre_stats = (
            ratings.map(lambda row: (row[1], row[2]))
            .join(movie_genres)
            .flatMap(lambda item: [(genre, (item[1][0], 1)) for genre in item[1][1]])
            .reduceByKey(sum_rating_counts)
            .mapValues(average_with_count)
            .map(lambda item: (item[0], item[1][0], item[1][1]))
            .sortBy(lambda row: (-row[1], row[0]))
            .collect()
        )
        bai2_text = format_table(
            "Bài 2 - Điểm trung bình theo thể loại",
            genre_stats,
            ["Genre", "AvgRating", "TotalRatings"],
        ).strip()
        print(bai2_text)
        write_output("bai2", bai2_text)

        user_gender = users.map(lambda row: (row[0], row[1])).cache()
        movie_gender_stats = (
            ratings_by_user.join(user_gender)
            .map(lambda item: ((item[1][0][0], item[1][1]), (item[1][0][1], 1)))
            .reduceByKey(sum_rating_counts)
            .mapValues(average_with_count)
            .map(lambda item: (item[0][0], (item[0][1], item[1][0], item[1][1])))
            .join(movie_titles)
            .map(lambda item: (item[0], item[1][1], item[1][0][0], item[1][0][1], item[1][0][2]))
            .sortBy(lambda row: (row[1], row[2]))
            .take(30)
        )
        bai3_text = format_table(
            "Bài 3 - Điểm trung bình theo giới tính",
            movie_gender_stats,
            ["MovieID", "Title", "Gender", "AvgRating", "TotalRatings"],
        ).strip()
        print(bai3_text)
        write_output("bai3", bai3_text)

        user_age_group = users.map(lambda row: (row[0], age_group(row[2]))).cache()
        movie_age_group_stats = (
            ratings_by_user.join(user_age_group)
            .map(lambda item: ((item[1][0][0], item[1][1]), (item[1][0][1], 1)))
            .reduceByKey(sum_rating_counts)
            .mapValues(average_with_count)
            .map(lambda item: (item[0][0], (item[0][1], item[1][0], item[1][1])))
            .join(movie_titles)
            .map(lambda item: (item[0], item[1][1], item[1][0][0], item[1][0][1], item[1][0][2]))
            .sortBy(lambda row: (row[1], row[2]))
            .take(30)
        )
        bai4_text = format_table(
            "Bài 4 - Điểm trung bình theo nhóm tuổi",
            movie_age_group_stats,
            ["MovieID", "Title", "AgeGroup", "AvgRating", "TotalRatings"],
        ).strip()
        print(bai4_text)
        write_output("bai4", bai4_text)

        user_occupation_id = users.map(lambda row: (row[0], row[3])).cache()
        occupation_stats = (
            ratings_by_user.join(user_occupation_id)
            .map(lambda item: (item[1][1], (item[1][0][1], 1)))
            .reduceByKey(sum_rating_counts)
            .mapValues(average_with_count)
            .join(occupations)
            .map(lambda item: (item[0], item[1][1], item[1][0][0], item[1][0][1]))
            .sortBy(lambda row: (-row[2], row[1]))
            .collect()
        )
        bai5_text = format_table(
            "Bài 5 - Điểm trung bình theo nghề nghiệp",
            occupation_stats,
            ["OccupationID", "Occupation", "AvgRating", "TotalRatings"],
        ).strip()
        print(bai5_text)
        write_output("bai5", bai5_text)

        year_stats = (
            ratings.map(lambda row: (year_from_unix(row[3]), (row[2], 1)))
            .reduceByKey(sum_rating_counts)
            .mapValues(average_with_count)
            .map(lambda item: (item[0], item[1][0], item[1][1]))
            .sortBy(lambda row: row[0])
            .collect()
        )
        bai6_table_text = format_table(
            "Bài 6 - Điểm trung bình theo năm",
            year_stats,
            ["Year", "AvgRating", "TotalRatings"],
        ).strip()
        bai6_text = "\n".join(
            [bai6_table_text, "", "Run all success: Hoàn thành 6 bài Spark RDD."]
        ).strip()
        print(bai6_table_text)
        write_output("bai6", bai6_text)

        run_text = "\n".join(
            [
                summary_text,
                "",
                bai6_text,
            ]
        ).strip()
        write_output("run_all_success", run_text)
        print("\nRun all success: Hoàn thành 6 bài Spark RDD.")
    finally:
        sc.stop()


if __name__ == "__main__":
    main()
