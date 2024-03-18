FROM apache/airflow:2.8.2-python3.10

COPY requirements.txt /opt/
WORKDIR /opt/

USER root
RUN apt-get update && \
    apt-get install -y zstd

USER airflow
RUN pip install -r requirements.txt --no-cache-dir
