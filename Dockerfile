# Dockerfile (Final)
FROM apache/airflow:2.8.4

USER root
RUN apt-get update && apt-get install -y --no-install-recommends unzip && apt-get clean

USER airflow
RUN pip install --no-cache-dir --upgrade pip

RUN pip install --no-cache-dir \
    clickhouse-connect \
    kaggle \
    pandas