from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, when, lit
import argparse
import sys
from pyspark.sql.utils import AnalysisException
from datetime import datetime

def arg_parse():
    parser = argparse.ArgumentParser()
    parser.add_argument('--checked_at', type=str, required=True)
    return parser.parse_args()

spark = SparkSession.builder \
        .appName("S3-Spark-Streaming") \
        .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .getOrCreate()

spark.conf.set("spark.sql.files.ignoreCorruptFiles", "true")

try:
    args = arg_parse()
    # checked_at = "2025-02-24T13:45:03" # 2025-02-24/13/45/
    checked_at = datetime.strptime(args.checked_at, '%Y-%m-%dT%H:%M:%S')
    checked_at_date = checked_at.strftime('%Y-%m-%d')
    hour = str(checked_at.hour)
    minute = str(checked_at.minute)
    date = f"{checked_at_date}/{hour}/{minute}"
    print(hour)
    print(minute)
    print(date)

except ValueError as e:
    print(f"Wrong date format: {args.checked_at}. ISO 8601 (YYYY-MM-DDTHH:MM:SS) format needed.", file=sys.stderr)
    sys.exit(1)

#     Parquet 파일 저장 (df, df2, summary 저장)
base_s3_path = "s3a://transform-emr/output"

try :
    # S3에서 Parquet 파일 읽기
    df = spark.read.option("mergeSchema", "true").parquet(f"s3a://extract-web/{date}/*_comments.parquet")
    df2 = spark.read.parquet(f"s3a://extract-web/{date}/*_posts.parquet")

    if df.rdd.isEmpty() or df2.rdd.isEmpty():
        print("Warning! Empty parquet", file=sys.stderr)

except AnalysisException as e:
    print(f"s3 parquet reading error {e}", file=sys.stderr)
    sys.exit(1)
    
# 게시글 데이터 저장
df2.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/posts/{checked_at_date}/{hour}/{minute}/posts.parquet")

# 댓글 데이터 저장
df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/comments/{checked_at_date}/{hour}/{minute}/comments.parquet")

df_columns = set(df.columns)
df2_columns = set(df2.columns)

missing_cols_df2 = list(df_columns - df2_columns)  # df2(게시글)에 없는 컬럼
missing_cols_df = list(df2_columns - df_columns)  # df(댓글)에 없는 컬럼


for col_name in missing_cols_df2:
    df2 = df2.withColumn(col_name, lit(None))

for col_name in missing_cols_df:
    df = df.withColumn(col_name, lit(None))

print(df.count())
print(df2.count())

# # 2️⃣ 동일한 컬럼 순서로 정렬 후 union
common_columns = sorted(list(df.columns))
df = df.select(common_columns)
df2 = df2.select(common_columns)

# 3️⃣ 두 데이터프레임을 통합 (checked_at이 동일하므로 groupBy 필요 없음)
combined_df = df2.union(df)

combined_df.count()

summary_df = combined_df.agg(
        sum("view").alias("total_views"),  # 전체 조회수 합산
        count("*").alias("total_posts_comments"),  # 전체 게시글 및 댓글 개수
        sum((col("sentiment") == "positive").cast("int")).alias("positive"),  # 긍정 개수
        sum((col("sentiment") == "neutral").cast("int")).alias("neutral"),  # 중립 개수
        sum((col("sentiment") == "negative").cast("int")).alias("negative")  # 부정 개수
    )

# 2️⃣ 사고 유형 집계
accidents = [
    ("화재", "fire"), ("멈춤", "stopped"), ("교통", "traffic"), ("충돌", "collision"),
    ("추돌", "rear_end"), ("전복", "rollover"), ("보행자", "pedestrian"), ("자전거", "bicycle"),
    ("이륜차", "motorcycle"), ("도로이탈", "road_departure"), ("신호위반", "signal_violation"),
    ("과속", "speeding"), ("급제동", "hard_braking"), ("급가속", "hard_acceleration"),
    ("급차선변경", "lane_change"), ("고장", "mechanical_failure"), ("타이어펑크", "tire_blowout"),
    ("침수", "flooding"), ("낙하물", "falling_object"), ("야생동물", "wildlife_collision")
]
for accident_kr, accident_en in accidents:
    combined_df = combined_df.withColumn(f"{accident_en}_count", when(col("content").contains(accident_kr), 1).otherwise(0))

accident_summary = combined_df.agg(
    *[sum(col(f"{accident_en}_count")).alias(f"{accident_en}_total_count") for _, accident_en in accidents]
)

# 3️⃣ 경쟁사 브랜드 언급 집계
brand_competitors = [("기아", "kia"), ("현대", "hyundai"), ("bmw", "bmw"), ("혼다", "honda"), ("아우디", "audi"), ("테슬라", "tesla")]
for competitor_kr, competitor_en in brand_competitors:
    combined_df = combined_df.withColumn(f"{competitor_en}_count", when(col("content").contains(competitor_kr), 1).otherwise(0))

brand_competitor_summary = combined_df.agg(
    *[sum(col(f"{competitor_en}_count")).alias(f"{competitor_en}_total_count") for _, competitor_en in brand_competitors]
)

# 1️⃣ 모델 유형 매핑 (한글 -> 영어)
model_competitors = [
    ("i4", "i4"),
    ("G80", "genessis"),
    ("타이칸", "taycan"),
    ("트론", "e_tron"),
    ("ev6", "ev6")
]

# 2️⃣ 모델 언급 개수 컬럼 추가
for competitor_kr, competitor_en in model_competitors:
    combined_df = combined_df.withColumn(
        f"{competitor_en}_count", when(col("content").contains(competitor_kr), 1).otherwise(0)
    )

# 3️⃣ 모델 경쟁사 언급 개수 집계 (전체 데이터 단위로 집계)
model_competitor_summary = combined_df.agg(
    *[
        sum(col(f"{competitor_en}_count")).alias(f"{competitor_en}_total_count")
        for _, competitor_en in model_competitors
    ]
)

# 4️⃣ 전기차 이슈 집계
ev_issue = [
    ("배터리", "battery"), ("방전", "drain"), ("충전", "charging"), ("전원 차단", "power_cutoff"),
    ("수명", "battery_degradation"), ("손실", "power_loss"), ("인버터", "inverter_failure"),
    ("모터", "motor_failure"), ("BMS", "bms_error"), ("소프트웨어", "software_bug"),
    ("자율주행", "autopilot_malfunction"), ("충전포트", "charging_port_failure"),
    ("에어컨", "hvac_failure"), ("냉각", "cooling_system_fault")
]
for ev_issue_kr, ev_issue_en in ev_issue:
    combined_df = combined_df.withColumn(f"{ev_issue_en}_count", when(col("content").contains(ev_issue_kr), 1).otherwise(0))

ev_issue_summary = combined_df.agg(
    *[sum(col(f"{ev_issue_en}_count")).alias(f"{ev_issue_en}_total_count") for _, ev_issue_en in ev_issue]
)

from functools import reduce

summary_df = summary_df.withColumn("checked_at", lit(checked_at))

def merge_dicts(x, y):
    x.update(y)
    return x

def df_to_dict(df):
    return df.first().asDict()

# Not WORKING at all times
# final_summary = (
#     summary_df
#     .crossJoin(accident_summary)
#     .crossJoin(brand_competitor_summary)
#     .crossJoin(model_competitor_summary)
#     .crossJoin(ev_issue_summary)
#     .withColumn("checked_at", lit(checked_at))
# )

try:
    all_dicts = [df_to_dict(accident_summary), df_to_dict(brand_competitor_summary), df_to_dict(model_competitor_summary), df_to_dict(ev_issue_summary)]
    merged_dict = reduce(merge_dicts, all_dicts, {})

    for col_name, col_value in merged_dict.items():
        summary_df = summary_df.withColumn(col_name, lit(col_value))

    final_summary = summary_df
    final_summary.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/summary/{checked_at_date}/{hour}/{minute}/summary.parquet")
except Exception as e:
    print(f"Agg concat Error: {e}", file=sys.stderr)
    sys.exit(1)