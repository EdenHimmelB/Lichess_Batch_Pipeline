FROM python:3.10.12
# Airflow needs a home, `/usr/local/airflow` is the conventional location
ENV AIRFLOW_HOME=/usr/local/airflow

# Install Airflow using the constraints to make sure all the dependencies are compatible
ARG AIRFLOW_VERSION=2.8.2
ARG CONSTRAINTS_URL="https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-3.8.txt"
RUN pip install "apache-airflow==${AIRFLOW_VERSION}" --constraint "${CONSTRAINTS_URL}"

# Copy your Airflow configuration and DAGs to the container
COPY ./config/airflow.cfg ${AIRFLOW_HOME}/airflow.cfg
COPY ./dags ${AIRFLOW_HOME}/dags

# Expose ports for webserver and potentially other services (e.g., Flower for Celery)
EXPOSE 8080

# Set the default command to run Airflow
CMD ["airflow", "webserver"]
