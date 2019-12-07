#!/usr/bin/env bash
export MONGODB_NODES=your_nodes
export MONGODB_ARGS=your_args
export MONGODB_CREDENTIALS=your_login:your_password

export MONGODB_DATABASE="hh-mongo"
export MONGODB_COLLECTION="raw_vacancies"

docker-compose -f docker-compose.kafka.yml -f docker-compose.etl.yml up --build
