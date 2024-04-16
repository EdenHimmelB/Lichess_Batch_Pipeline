FROM apache/airflow:2.8.2

WORKDIR /opt/
COPY requirements.txt /opt/

USER airflow
RUN pip install -r requirements.txt --no-cache-dir

USER root
RUN mkdir -p /opt/airflow/spark-lib
RUN apt-get update && \
    apt-get install -y openjdk-17-jre zstd && \
    # Delete apt cache to reduce image size
    rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
RUN export JAVA_HOME

# Download GCS Connector so that Airflow can distribute them to Spark nodes during spark job submission.
ADD https://repo1.maven.org/maven2/com/google/cloud/bigdataoss/gcs-connector/hadoop3-2.2.21/gcs-connector-hadoop3-2.2.21-shaded.jar /opt/airflow/spark-lib
