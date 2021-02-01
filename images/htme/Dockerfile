FROM openjdk:8-jre

RUN mkdir config
COPY keystore.jks ./
COPY truststore.jks ./
COPY application.properties config/
COPY ./hbase-to-mongo-export.jar ./

ENTRYPOINT [ "java", "-jar", "./hbase-to-mongo-export.jar"]
