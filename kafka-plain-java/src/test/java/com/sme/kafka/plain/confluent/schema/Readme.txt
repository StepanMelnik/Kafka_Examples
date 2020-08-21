# start confluent docker compose
/opt/confluent/cp-all-in-one/cp-all-in-one$ sudo docker-compose up


#Error:
#ERROR: for zookeeper  Cannot create container for service zookeeper: Conflict. The container name "/zookeeper" is already in use by container "7410aaa1db7fbbf8dc0711cc3bd139536827ff27040d95e8a1d98f189e28da00". You have to remove (or rename) that container to be able to reuse that name.
sudo docker images -a | grep zookeeper
# remove all docker images: confluentinc/cp-zookeeper
sudo docker image rm -f 124ff6469e3d

# check all running containers
sudo docker ps -a | grep zookeeper
# remove all zookeeper containers
sudo docker rm --force 124ff6469e3d


# Stops containers and removes containers and named volumes
docker-compose down --volume

# start confluent docker compose again
/opt/confluent/cp-all-in-one/cp-all-in-one$ sudo docker-compose up
# all services should be started

# check all service by UI:
* zookeper: http://192.168.0.109:2181/
* broker: http://192.168.0.109:9092, http://192.168.0.109:9101/
* schema-registry: http://192.168.0.109:8081/
* cnfldemo: http://192.168.0.109:8083/
* controlcenter: http://192.168.0.109:9021/clusters
* ksqldbserver: http://192.168.0.109:8088/info

# connect to broker and check list of topics:
 sudo docker container ls | grep broker
 sudo docker exec -it dda35e2ed6d4 /bin/bash
 > ps aux | grep java <-- the process created as java process (no kafka*.sh)
 
 Avro spec:
 https://avro.apache.org/docs/1.7.7/spec.html#Schema+Resolution
 
 Schema (https://docs.confluent.io/current/schema-registry/schema_registry_tutorial.html):
 http://192.168.0.109:8081/subjects/ <-- all subjects
 http://192.168.0.109:8081/subjects/transactions-value/versions/latest
 
 
