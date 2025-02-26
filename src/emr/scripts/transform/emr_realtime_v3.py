from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, count, to_date, when, lit
import argparse
import sys
from pyspark.sql.utils import AnalysisException
from pyspark.sql.functions import expr, col, when, sum
from datetime import datetime
import time

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
except ValueError as e:
    print(f"Wrong date format: {args.checked_at}. ISO 8601 (YYYY-MM-DDTHH:MM:SS) format needed.", file=sys.stderr)
    sys.exit(1)

#     Parquet 파일 저장 (df, df2, summary 저장)
base_s3_path = "s3a://transform-emr/output"

# S3에서 Parquet 파일 읽기
try:
    df = spark.read.option("mergeSchema", "true").parquet(f"s3a://extract-web/{date}/*.parquet")    
    if df.rdd.isEmpty():
        print("Warning! Empty parquet", file=sys.stderr)
    
    # 필요한 컬럼만 선택
    df = df.select(
        "platform",
        "title",
        "post_id",
        "url",
        "content",
        "view",
        "created_at",
        "like",
        "dislike",
        "comment_count",
        "keywords",
        "sentiment",
        "checked_at"
    )
        
except AnalysisException as e:
    print(f"s3 parquet reading error {e}", file=sys.stderr)
    sys.exit(1)
    
# 통합 데이터 저장
df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/posts/{checked_at_date}/{hour}/{minute}/posts.parquet")

# 사고 유형 집계 컬럼
accidents = [
    ("화재", "fire"), ("멈춤", "stopped"), ("교통", "traffic"), ("충돌", "collision"),
    ("추돌", "rear_end"), ("전복", "rollover"), ("보행자", "pedestrian"), ("자전거", "bicycle"),
    ("이륜차", "motorcycle"), ("도로이탈", "road_departure"), ("신호위반", "signal_violation"),
    ("과속", "speeding"), ("급제동", "hard_braking"), ("급가속", "hard_acceleration"),
    ("급차선변경", "lane_change"), ("고장", "mechanical_failure"), ("타이어펑크", "tire_blowout"),
    ("침수", "flooding"), ("낙하물", "falling_object"), ("야생동물", "wildlife_collision")
]
accident_columns = [] 
for accident_kr, accident_en in accidents:
    accident_columns.append(f"case when content like '%{accident_kr}%' then 1 else 0 end as {accident_en}_count")

# 브랜드 경쟁사 언급 집계 컬럼
brand_competitors = [("기아", "kia"), ("현대", "hyundai"), ("bmw", "bmw"), ("혼다", "honda"), ("아우디", "audi"), ("테슬라", "tesla")]
brand_columns = []
for competitor_kr, competitor_en in brand_competitors:
    brand_columns.append(f"case when content like '%{competitor_kr}%' then 1 else 0 end as {competitor_en}_count")

# 모델 경쟁사 언급 집계 컬럼
model_competitors = [("i4", "i4"), ("G80", "genessis"), ("타이칸", "taycan"), ("트론", "e_tron"), ("ev6", "ev6")]
model_columns = []
for competitor_kr, competitor_en in model_competitors:
    model_columns.append(f"case when content like '%{competitor_kr}%' then 1 else 0 end as {competitor_en}_count")

# 전기차 이슈 집계
ev_issue = [
    ("배터리", "battery"), ("방전", "drain"), ("충전", "charging"), ("전원 차단", "power_cutoff"),
    ("수명", "battery_degradation"), ("손실", "power_loss"), ("인버터", "inverter_failure"),
    ("모터", "motor_failure"), ("BMS", "bms_error"), ("소프트웨어", "software_bug"),
    ("자율주행", "autopilot_malfunction"), ("충전포트", "charging_port_failure"),
    ("에어컨", "hvac_failure"), ("냉각", "cooling_system_fault")
]
ev_issue_columns = []
for ev_issue_kr, ev_issue_en in ev_issue:
    ev_issue_columns.append(f"case when content like '%{ev_issue_kr}%' then 1 else 0 end as {ev_issue_en}_count")

df = df.selectExpr("*", *accident_columns, *brand_columns, *model_columns, *ev_issue_columns)

# 모든 집계를 하나의 agg() 호출에 통합
all_metrics_summary = df.agg(
    # 기본 지표
    sum("view").alias("total_views"),
    count("*").alias("total_posts_comments"),
    sum((col("sentiment") == "positive").cast("int")).alias("positive"),
    sum((col("sentiment") == "neutral").cast("int")).alias("neutral"),
    sum((col("sentiment") == "negative").cast("int")).alias("negative"),
    
    # 사고 유형 집계
    *[sum(expr(f"{accident_en}_count")).alias(f"{accident_en}_total_count") 
      for _, accident_en in accidents],
    
    # 브랜드 경쟁사 언급 집계
    *[sum(col(f"{competitor_en}_count")).alias(f"{competitor_en}_total_count") 
      for _, competitor_en in brand_competitors],
    
    # 모델 경쟁사 언급 집계
    *[sum(col(f"{competitor_en}_count")).alias(f"{competitor_en}_total_count") 
      for _, competitor_en in model_competitors],
    
    # 전기차 이슈 집계
    *[sum(col(f"{ev_issue_en}_count")).alias(f"{ev_issue_en}_total_count") 
      for _, ev_issue_en in ev_issue]
)

try:
    # checked_at 열이 있는 데이터프레임 생성
    checked_at_df = spark.createDataFrame([(checked_at,)], ["checked_at"])
    
    # 두 데이터프레임을 직접 조인 - 둘 다 단일 행이므로 효율적
    final_df = checked_at_df.crossJoin(all_metrics_summary)
    
    # 결과 저장
    final_df.coalesce(1).write.mode("overwrite").parquet(f"{base_s3_path}/summary/{checked_at_date}/{hour}/{minute}/summary.parquet")
except Exception as e:
    print(f"Agg concat Error: {e}", file=sys.stderr)
    sys.exit(1)