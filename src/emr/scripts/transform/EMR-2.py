from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType
from pyspark.sql.functions import col, sum
import argparse
from datetime import datetime

def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checked_at', type=str, required=True)
    return parser.parse_args()

def main():
    spark = SparkSession.builder \
    .appName("S3-Spark-Streaming2") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

    spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

    args = parse_arguments()

    checked_at = datetime.strptime(args.checked_at, '%Y-%m-%dT%H:%M:%S')
    checked_at_date = checked_at.strftime('%Y-%m-%d')
    hour = str(checked_at.hour)
    minute = str(checked_at.minute)

    date = f"{checked_at_date}/{hour}/{minute}"

    # S3에서 Parquet 파일 읽기
    df = spark.read.option("mergeSchema", "true").parquet(f"s3a://extract-web/{date}/*_comments.parquet")
    df2 = spark.read.parquet(f"s3a://extract-web/{date}/*_posts.parquet")

    total_post_comments = df.count() + df2.count()

    total_views = df2.select("view").agg(sum("view")).collect()[0][0]

    # Positive 게시글 및 댓글 수 세기
    positive_comment = df.filter(col("sentiment") == "positive").count()
    positive_post = df2.filter(col("sentiment") == "positive").count()
    total_positive = positive_comment + positive_post

    # Neutral 게시글 및 댓글 수 세기
    neutral_comment = df.filter(col("sentiment") == "neutral").count()
    neutral_post = df2.filter(col("sentiment") == "neutral").count()
    total_neutral = neutral_comment + neutral_post

    # Negative 게시글 및 댓글 수 세기
    negative_comment = df.filter(col("sentiment") == "negative").count()
    negative_post = df2.filter(col("sentiment") == "negative").count()
    total_negative = negative_comment + negative_post

    # 2️⃣ 데이터 스키마 정의
    schema = StructType([
        StructField("checked_at", TimestampType(), True),
        StructField("total_views", IntegerType(), True),
        StructField("total_posts_comments", IntegerType(), True),
        StructField("positive", IntegerType(), True),
        StructField("neutral", IntegerType(), True),
        StructField("negative", IntegerType(), True),
    ])

    data = [
    (checked_at, total_views, total_post_comments, total_positive, total_neutral, total_negative)
    ]

    # 4️⃣ Spark DataFrame 생성
    output = spark.createDataFrame(data, schema=schema)

    # 4️⃣ Parquet 파일 저장 (df, df2, output 저장)
    base_s3_path = "s3a://transform-emr"

    # 1️⃣ 전체 감성 분석 통계 저장
    output.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/summary/{checked_at_date}/{hour}/{minute}/summary.parquet") # 경로가 디렉토리임

    # 2️⃣ 게시글 데이터 저장
    df2.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/posts/{checked_at_date}/{hour}/{minute}/posts.parquet") # 경로가 디렉토리임

    # 3️⃣ 댓글 데이터 저장
    df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/comments/{checked_at_date}/{hour}/{minute}/comments.parquet") # 경로가 디렉토리임
    
if __name__ == '__main__' :
    main()