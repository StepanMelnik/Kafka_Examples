start kafka, zookeeper, ksqldb:
https://ksqldb.io/quickstart.html


Docker:
# all running containers
docker container ls
docker volume ls

sudo docker container ls | grep confluentinc


# all images
sudo docker images -a
docker images -a | grep kafka

Docker-compose:
docker-compose up

# check all docker images in the docker compose
sudo docker container ls | grep confluentinc

# connect to kafka image
sudo docker exec -it broker /bin/bash
# or:
sudo docker exec -it 456931d1bca1 /bin/bash
ps aux | grep java
exec

# connect to ksqldb-server
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
http://localhost:8088/info

# Java client:
https://docs.ksqldb.io/en/latest/developer-guide/ksqldb-clients/


# Problems with ksql db server. How to check logs:
sudo docker container ls | grep ksqldb
sudo docker exec -it  ksqldb-server /bin/bash
> cd /etc/ksqldb-server <-- log4j uses console appender only
docker logs -f ksqldb-server
