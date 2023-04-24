# A micro data architecture

This implementation follows the course Confluent Kafka Fundamentals. 
The aim of this repository is to implement a PoC of a testable architecture based on Kafka.
An extension considering Spark as a data development tool for ingestion is under development.

## Set up
The Kafka cluster can be spin up using the command

```
./gradlew composeUp
```


## A simple application based on Kafka Stream

```
./gradlew createKafkaTopics
```
on successful completion, two topics will be shown on the UI accessible at [localhost:9021](http://localhost:9021) 
* vehicle-position
* vehicle-position-stream-out

`vehicle-position` topic is used as input stream to `VehiclePositionProcessor` processor which translates a 2D point
on the horizontal axis. The result is sent on the topic `vehicle-position` for further processing.

