# Q5: 커스텀 Airflow 이미지 (공식 이미지 + JDK + 파이썬 패키지)
FROM apache/airflow:3.3.0

# 1) JDK 설치 — apt 로 패키지를 설치하려면 root 권한이 필요함
#    procps: Spark 실행 스크립트가 사용하는 ps 명령
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jdk-headless procps \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 2) requirements.txt 로 파이썬 패키지 설치 — pip 는 airflow 사용자로 실행
#    apache-airflow 를 현재 버전으로 함께 적어서, 패키지 설치 중 Airflow 버전이 바뀌지 않게 고정
USER airflow
COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /requirements.txt
