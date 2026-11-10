import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

# 버킷 이름은 코드에 적지 않고 환경변수로 받음 (project/.env → 컨테이너 환경변수 S3_BUCKET)
# AWS 키도 코드·yaml·.env 에 적지 않음 → Airflow UI 에 등록한 Connection(aws_default) 사용
AWS_CONN_ID = "aws_default"
SOURCE_KEY = "bronze/netflix_titles.csv"

# 컨테이너 안 작업 폴더 (실행 날짜 ds 별로 분리)
WORK_DIR = "/opt/airflow/weekly/{ds}"


def get_bucket():
    bucket = os.environ.get("S3_BUCKET")
    if not bucket:
        raise ValueError("환경변수 S3_BUCKET 이 설정되지 않았습니다.")
    return bucket


def download_csv(**context):
    bucket = get_bucket()
    work_dir = WORK_DIR.format(ds=context["ds"])
    os.makedirs(work_dir, exist_ok=True)
    local_path = f"{work_dir}/netflix_titles.csv"

    # S3Hook.get_conn() 은 boto3 S3 client 를 돌려줌
    s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()
    s3.download_file(bucket, SOURCE_KEY, local_path)
    print(f"downloaded s3://{bucket}/{SOURCE_KEY} -> {local_path} ({os.path.getsize(local_path):,} bytes)")


def upload_silver(**context):
    bucket = get_bucket()
    ds = context["ds"]
    output_dir = f"{WORK_DIR.format(ds=ds)}/silver"
    prefix = f"silver/{ds}/"

    s3 = S3Hook(aws_conn_id=AWS_CONN_ID).get_conn()

    # Spark 가 만든 parquet 파일만 업로드 (.crc 같은 부속 파일 제외)
    for file_name in sorted(os.listdir(output_dir)):
        if file_name.endswith(".parquet"):
            s3.upload_file(f"{output_dir}/{file_name}", bucket, prefix + file_name)
            print(f"uploaded {file_name} -> s3://{bucket}/{prefix}{file_name}")

    # 업로드 결과: 목록과 개수 출력
    response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)
    objects = response.get("Contents", [])
    print(f"list s3://{bucket}/{prefix}")
    for obj in objects:
        print(f"    {obj['Key']}  {obj['Size']:,} bytes")
    print(f"object count = {len(objects)}")


with DAG(
    dag_id="weekly_pipeline_leechanmin",
    start_date=datetime(2026, 11, 9),   # 지난 주기(11/8 일요일)가 자동 실행되지 않도록 그 다음 날로 지정
    schedule="@weekly",                 # 매주 1회 (일요일 0시, UTC)
    catchup=False,
    params={"base_year": 2015},   # 기준 연도 — transform.py 에 실행 인자로 주입 (트리거할 때 바꿀 수 있음)
) as dag:

    # 1) 다운로드: S3 bronze → 컨테이너
    download_task = PythonOperator(
        task_id="download_csv",
        python_callable=download_csv,
    )

    # 2) 집계: spark-submit 으로 jobs/transform.py 실행
    transform_task = BashOperator(
        task_id="transform",
        bash_command=(
            "spark-submit --master local[*] /opt/airflow/dags/jobs/transform.py "
            "--input /opt/airflow/weekly/{{ ds }}/netflix_titles.csv "
            "--output /opt/airflow/weekly/{{ ds }}/silver "
            "--base-year {{ params.base_year }}"
        ),
    )

    # 3) 업로드: 결과 parquet → S3 silver/{날짜}/
    upload_task = PythonOperator(
        task_id="upload_silver",
        python_callable=upload_silver,
    )

    download_task >> transform_task >> upload_task
