import psycopg2
import os
from dotenv import load_dotenv

dotenv_lambda_path = ".env.lambda"
print(f"[INFO] .env 경로 확인: {dotenv_lambda_path}")
print("[INFO] 경로에 .env.lambda가 있나요?",os.path.exists(dotenv_lambda_path))

load_dotenv(dotenv_lambda_path)

DB_HOST = os.getenv("DB_HOST")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_PORT = os.getenv("DB_PORT")
VIEW_THRESHOLD = int(os.getenv("VIEW_THRESHOLD"))
S3_BUCKET = os.getenv("S3_BUCKET")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


print("=== 환경변수 확인 ===")
print(f"DB_HOST: {DB_HOST}")
print(f"DB_NAME: {DB_NAME}")
print(f"DB_USER: {DB_USER}")
print(f"DB_PORT: {DB_PORT}")