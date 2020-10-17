FROM dwp-centos-with-java-htme:latest

RUN mkdir -p certs config
COPY htme-keystore.jks certs/keystore.jks
COPY htme-truststore.jks certs/truststore.jks
COPY *.properties config/
COPY ./hbase-to-mongo-export.jar ./
RUN ls -la *.jar


ENTRYPOINT ["sh", "-c", "java -Dcorrelation_id=${CORRELATION_ID} -Dsqs_message_id=${SQS_MESSAGE_ID} -Dtopic_name=${TOPIC_NAME} -Denvironment=${ENVIRONMENT} -Dapplication=${APPLICATION} -Dapp_version=${APP_VERSION} -Dcomponent=${COMPONENT} -jar ./hbase-to-mongo-export-latest.jar \"$@\"", "--"]
