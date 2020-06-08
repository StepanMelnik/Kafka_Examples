# Kafka_Examples
Kafka examples: Docker to start Kafka, Kafka in plain java, Kafka in SpringBoot.

## Description
The project has a few examples how to start to work with <a href="https://kafka.apache.org/">Kafka</a> stream platform.

The following parts implemened in the project:
* The docker part describes how to start Zookeeper and Kafka in Docker container.
* "Kafka in plain java" module works with Kafka API.
* "Kafka in SpringBoot" works with Spring Boot.

## Build

Clone and install <a href="https://github.com/StepanMelnik/Parent.git">Parent</a> project before building.

	### Docker
		Check Header in the Dockerfile how to create and run an image of the project.

	### Maven
		> mvn clean install

	### Jenkins
		Check Jenkins file.


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

All of cases covered by unit tests. Create ''kafka-plain-java/src/main/resources/config.json'' file before running unit tests.

	### Init properties
				sh "rm -f config.json> /dev/null"
				sh '''
					echo { >> config.json
					echo "host" : "192.168.0.109:9092", >> config.json
					echo "topic" : "HelloKafka" >> config.json
					echo } >> config.json
				'''
				sh "cp config.json kafka-plain-java/src/main/resources/config.json"


## Kafka in SpringBoot
TODO


