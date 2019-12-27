#!/usr/bin/env bash
export MONGODB_NODES=your_nodes
export MONGODB_ARGS=your_args
export MONGODB_CREDENTIALS=your_login:your_password

export MONGODB_DATABASE="hh-mongo"
export MONGODB_COLLECTION="raw_vacancies"

kompose convert -f docker-compose.kafka.yml -f docker-compose.etl.yml

# deploy Kafka instances and zookeeper
kubectl create -f kafka-1-service.yaml,kafka-2-service.yaml,zookeeper-service.yaml,kafka-1-deployment.yaml,kafka-2-deployment.yaml,zookeeper-deployment.yaml

# deploy connector
kubectl create -f connector-deployment.yaml

# deploy spark streaming job
kubectl create -f spark-job-deployment.yaml

rm kafka-1-service.yaml kafka-2-service.yaml zookeeper-service.yaml kafka-1-deployment.yaml kafka-2-deployment.yaml zookeeper-deployment.yaml
rm connector-deployment.yaml spark-job-deployment.yaml