FROM openjdk:8-jdk-alpine
ADD target/kafka-partitions.jar kafka-partitions.jar
EXPOSE 9002
ENTRYPOINT ["java","-jar","kafka-partitions.jar"]