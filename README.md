# Kafka_Examples
Kafka examples: Docker to start Kafka, Kafka in shell, Kafka in plain java, Kafka in SpringBoot

## Docker

Start Kafka 2.5.0 server: 
> sudo docker-compose -f docker-compose.yml up -d

Start a cluster with three brokers:
> sudo docker-compose scale kafka=3 

Stop Kafka 2.5.0 server: 
> sudo docker-compose stop

Investigate Kafka image:
> sudo chmod +x kafka-shell.sh

> sudo sh ./kafka-shell.sh 192.168.0.199 192.168.0.199:2181

> cd /opt/kafka/config

Check more information in the header of docker-compose.yml

## Kafka in plain java
"kafka-plain-java" maven module demonstrates how to work with Kafka Consumer and Producer.

Also the module uses Admin and Stream API of Kafka implementation to work with Topics and Streams in runtime.

## Kafka in SpringBoot
TODO


