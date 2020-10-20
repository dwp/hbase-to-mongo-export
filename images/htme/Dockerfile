FROM openjdk:8-jre

RUN mkdir -p certs config
COPY keystore.jks ./
COPY truststore.jks ./
COPY *.properties config/
RUN ls -la config/*.properties
COPY ./hbase-to-mongo-export.jar ./

ENTRYPOINT ["sh", "-c", "java -Dcorrelation_id=${CORRELATION_ID} -Dsqs_message_id=${SQS_MESSAGE_ID} -Dtopic_name=${TOPIC_NAME} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT} -jar ./hbase-to-mongo-export.jar \"$@\"", "--"]
