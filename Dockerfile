FROM dwp-centos-with-java-htme:latest
ARG HBASE_TO_MONGO_EXPORT_VERSION

RUN mkdir -p certs config
COPY archive/htme-keystore.jks certs/keystore.jks
COPY archive/htme-truststore.jks certs/truststore.jks
COPY images/export/* config/
ENV APP_VERSION=${HBASE_TO_MONGO_EXPORT_VERSION}
ENV JAR=hbase-to-mongo-export-${APP_VERSION}.jar
COPY build/libs/$JAR ./hbase-to-mongo-export-latest.jar
RUN ls -la *.jar


ENTRYPOINT ["sh", "-c", "java -Dcorrelation_id=${CORRELATION_ID} -Dsqs_message_id=${SQS_MESSAGE_ID} -Dtopic_name=${TOPIC_NAME} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT} -jar ./hbase-to-mongo-export-latest.jar \"$@\"", "--"]
