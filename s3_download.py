import csv
import os
import sys

import boto3

# 버킷 이름은 코드에 적지 않고 실행 인자 또는 환경변수(S3_BUCKET)로 받음
#   실행 예) python3 s3_download.py de-8-leechanmin
if len(sys.argv) > 1:
    bucket = sys.argv[1]
else:
    bucket = os.environ.get("S3_BUCKET")

if not bucket:
    print("사용법: python3 s3_download.py <버킷이름>  (또는 환경변수 S3_BUCKET 지정)")
    sys.exit(1)

PREFIX = "bronze/"
KEY = "bronze/netflix_titles.csv"

# 다운로드 위치: 이 스크립트가 있는 폴더(project) 아래의 data/
#   __file__ : 지금 실행 중인 파이썬 파일의 경로
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOCAL_DIR = os.path.join(BASE_DIR, "data")
LOCAL_PATH = os.path.join(LOCAL_DIR, "netflix_titles.csv")

# 키는 코드에 적지 않음 — boto3 가 PC 의 AWS CLI 설정(~/.aws, IAM 사용자 키)을 자동으로 사용
s3 = boto3.client("s3")

# 1) bronze/ 아래 객체 목록과 각 객체의 크기 출력
print(f"[1] list s3://{bucket}/{PREFIX}")
response = s3.list_objects_v2(Bucket=bucket, Prefix=PREFIX)
for obj in response.get("Contents", []):
    print(f"    {obj['Key']:<35} {obj['Size']:>12,} bytes")

# 2) netflix_titles.csv 를 project/data/ 아래로 다운로드
os.makedirs(LOCAL_DIR, exist_ok=True)
print(f"[2] download s3://{bucket}/{KEY} -> data/netflix_titles.csv")
s3.download_file(bucket, KEY, LOCAL_PATH)
print(f"    downloaded ({os.path.getsize(LOCAL_PATH):,} bytes)")

# 3) 내려받은 파일의 행 수 = CSV 레코드 수 (헤더 제외)
#    csv 모듈은 따옴표로 묶인 값 안의 줄바꿈을 한 레코드로 처리함
with open(LOCAL_PATH, newline="", encoding="utf-8") as f:
    reader = csv.reader(f)
    next(reader)   # 헤더 한 줄 건너뛰기
    row_count = 0
    for row in reader:
        row_count += 1

print(f"[3] rows (CSV records, header excluded) = {row_count:,}")
