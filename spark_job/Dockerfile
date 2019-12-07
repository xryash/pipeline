FROM ubuntu:latest


# Python
RUN \
    apt-get update && \
    apt-get install -y python python-dev  python-pip  python-virtualenv && \
    rm -rf /var/lib/apt/lists/*

# Install dependencies
RUN \
    pip install kafka-python

# OpenJDK 8
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*


# Apache Spark
COPY spark /

# Python script for running Spark
COPY spark_job.py /

