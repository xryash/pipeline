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
  apt-get install -y openjdk-8-jdk wget gnupg2 && \
  rm -rf /var/lib/apt/lists/*


# Apache Spark
RUN wget --no-verbose http://mirror.linux-ia64.org/apache/spark/spark-2.4.4/spark-2.4.4-bin-hadoop2.7.tgz


RUN tar -xzf /spark-2.4.4-bin-hadoop2.7.tgz && \
    mv spark-2.4.4-bin-hadoop2.7 /spark && \
    echo "export PATH=$PATH:/spark/bin" >> ~/.bashrc

# Python script for running Spark
COPY spark_job.py /

