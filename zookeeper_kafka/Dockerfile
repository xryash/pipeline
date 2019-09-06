FROM openjdk

ARG brokerId=1

# There is must be Apache Kafka in this directory
COPY kafka /kafka

WORKDIR kafka

RUN sed -i s/localhost:2181/zookeeper:2181/g /kafka/config/server.properties && \
    sed -i s/broker.id=0/broker.id=${brokerId}/g /kafka/config/server.properties

EXPOSE 2181 9092
