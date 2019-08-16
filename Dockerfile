FROM dwp-centos-with-java-htme:latest
ARG HBASE_TO_MONGO_EXPORT_VERSION

RUN mkdir certs
COPY resources/htme-keystore.jks certs/keystore.jks
COPY resources/htme-truststore.jks certs/truststore.jks

ENV JAR=hbase-to-mongo-export-${HBASE_TO_MONGO_EXPORT_VERSION}.jar
COPY build/libs/$JAR $INSTALL_DIR/

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}
USER ${SERVICE_USER}

ENTRYPOINT ["sh", "-c", "${INSTALL_DIR}/${JAR} \"$@\"", "--"]
