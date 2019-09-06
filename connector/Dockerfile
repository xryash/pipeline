FROM ubuntu:latest

# Python
RUN \
    apt-get update && \
    apt-get install -y python3 python3-dev python3-pip  python3-virtualenv && \
    rm -rf /var/lib/apt/lists/*


# Install dependencies
RUN \
    pip3 install kafka-python requests


COPY connector.py /

