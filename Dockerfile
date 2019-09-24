FROM dwp-centos-with-java-htme:latest
ARG HBASE_TO_MONGO_EXPORT_VERSION

RUN mkdir -p certs
COPY resources/htme-keystore.jks certs/keystore.jks
COPY resources/htme-truststore.jks certs/truststore.jks

ENV JAR=hbase-to-mongo-export-${HBASE_TO_MONGO_EXPORT_VERSION}.jar
COPY build/libs/$JAR ./hbase-to-mongo-export-latest.jar
RUN ls -la *.jar

RUN chmod -R a+rwx /opt/hbase-to-mongo-export/data

ENTRYPOINT ["sh", "-c", "./hbase-to-mongo-export-latest.jar \"$@\"", "--"]
