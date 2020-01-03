FROM dwp-centos-with-java-htme:latest
ARG HBASE_TO_MONGO_EXPORT_VERSION

RUN mkdir -p certs
COPY resources/htme-keystore.jks certs/keystore.jks
COPY resources/htme-truststore.jks certs/truststore.jks

ENV APP_VERSION=${HBASE_TO_MONGO_EXPORT_VERSION}
ENV JAR=hbase-to-mongo-export-${APP_VERSION}.jar
COPY build/libs/$JAR ./hbase-to-mongo-export-latest.jar
RUN ls -la *.jar

RUN chmod -R a+rwx /opt/hbase-to-mongo-export/data

ENTRYPOINT ["sh", "-c", "java -Dtopic_name=${TOPIC_NAME} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT} -jar ./hbase-to-mongo-export-latest.jar \"$@\"", "--"]
