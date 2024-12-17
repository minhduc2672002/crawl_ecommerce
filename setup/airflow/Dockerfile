FROM apache/airflow:2.10.3-python3.12
USER root
RUN apt-get update \
  && apt-get install -y --no-install-recommends \
         openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*
RUN mkdir -p /opt/bitnami/spark
USER airflow
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
COPY requirements.txt .
RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r requirements.txt