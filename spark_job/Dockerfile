FROM ubuntu:latest


# Python
RUN \
    apt-get update && \
    apt-get install -y python python-dev && \
    rm -rf /var/lib/apt/lists/*

# OpenJDK 8
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*


# Apache Spark
COPY spark /

# Python script for running Spark
COPY spark_job.py /

