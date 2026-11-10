import os
from datetime import datetime

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

# 산출 위치: 컨테이너 안의 경로 (호스트 폴더와 연결하지 않음)
OUTPUT_DIR = "/opt/airflow/backfill_output"


def write_daily_file(**context):
    # ds: 이 실행의 logical date (YYYY-MM-DD)
    ds = context["ds"]
    os.makedirs(OUTPUT_DIR, exist_ok=True)

    file_path = f"{OUTPUT_DIR}/daily_{ds}.txt"
    with open(file_path, "w") as f:
        f.write(f"logical date: {ds}\n")
    print(f"written -> {file_path}")


def verify_file(**context):
    ds = context["ds"]
    file_path = f"{OUTPUT_DIR}/daily_{ds}.txt"
    with open(file_path) as f:
        print(f"{file_path} : {f.read().strip()}")


with DAG(
    dag_id="backfill_demo_leechanmin",
    start_date=datetime(2026, 11, 4),   # 작업일(2026-11-11) 기준 7일 전으로 고정
    schedule="@daily",                  # 매일 1회
    catchup=True,                       # DAG 를 켜면 start_date 부터 지난 구간 실행을 자동으로 채움
) as dag:

    write_daily_file_task = PythonOperator(
        task_id="write_daily_file",
        python_callable=write_daily_file,
    )

    verify_file_task = PythonOperator(
        task_id="verify_file",
        python_callable=verify_file,
    )

    write_daily_file_task >> verify_file_task
