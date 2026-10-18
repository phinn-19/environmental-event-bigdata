
# Moi truong Python
FROM python:3.11-slim-bookworm

# Cai Java 17 cho Spark
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless && \
    rm -rf /var/lib/apt/lists/*

# Cau hinh Spark su dung Python trong container
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PYSPARK_PYTHON=/usr/local/bin/python
ENV PYSPARK_DRIVER_PYTHON=/usr/local/bin/python
ENV SPARK_LOCAL_IP=127.0.0.1

# Cai PySpark va thu vien doc YAML
RUN pip install --no-cache-dir \
    pyspark==3.5.8 PyYAML==6.0.3

WORKDIR /app
