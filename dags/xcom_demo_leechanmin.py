from datetime import datetime, timedelta

from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator

TARGET_FILE = "/opt/airflow/dags/sample_dag.py"


def count_lines(**context):
    ti = context["ti"]
    print(f"try_number = {ti.try_number}")

    # (3) 재시도 확인용: 첫 번째 시도에서만 일부러 실패시킴
    if ti.try_number == 1:
        raise Exception("첫 시도라서 일부러 실패합니다. retry_delay 뒤에 재시도됩니다.")

    # (1) 값 하나 계산: 파일의 줄 수
    with open(TARGET_FILE) as f:
        line_count = len(f.readlines())
    print(f"{TARGET_FILE} line count = {line_count}")

    # return 한 값은 XCom 에 key='return_value' 로 자동 저장됨
    return line_count


def use_value(**context):
    ti = context["ti"]

    # 앞 task(count_lines)가 XCom 에 저장한 return 값을 꺼내옴
    value = ti.xcom_pull(task_ids="count_lines")
    print(f"received from XCom: {value}")
    print(f"doubled = {value * 2}")


with DAG(
    dag_id="xcom_demo_leechanmin",
    start_date=datetime(2026, 11, 1),
    schedule=None,   # 수동 실행
    catchup=False,
) as dag:

    count_lines_task = PythonOperator(
        task_id="count_lines",
        python_callable=count_lines,
        retries=2,                           # 실패하면 최대 2번 더 시도
        retry_delay=timedelta(seconds=10),   # 재시도 전 10초 대기
    )

    use_value_task = PythonOperator(
        task_id="use_value",
        python_callable=use_value,
    )

    count_lines_task >> use_value_task
