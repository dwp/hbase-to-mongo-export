FROM zenika/kotlin:1.3-jdk8-slim as buildImage

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/${SERVICE_USER}
ENV BUILD_DIR=/opt/hbase-to-mongo-export
ENV GRADLE='./gradlew --no-daemon'

RUN mkdir -p ${SERVICE_USER_HOME} ${BUILD_DIR}
RUN useradd -d ${SERVICE_USER_HOME} ${SERVICE_USER}
RUN id -a ${SERVICE_USER}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}

WORKDIR ${BUILD_DIR}

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY gradlew .
COPY gradle/ ./gradle

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${BUILD_DIR}

USER ${SERVICE_USER}

RUN $GRADLE wrapper
RUN $GRADLE --refresh-dependencies compileKotlin
COPY src/ ./src
RUN $GRADLE build -x test

FROM openjdk:8-slim
ARG HBASE_TO_MONGO_EXPORT_VERSION

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/${SERVICE_USER}
ENV INSTALL_DIR=/opt/hbase-to-mongo-export
RUN mkdir -p ${SERVICE_USER_HOME} ${INSTALL_DIR}/data
RUN useradd -d ${SERVICE_USER_HOME} ${SERVICE_USER}
RUN id -a ${SERVICE_USER}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}

WORKDIR ${INSTALL_DIR}
ENV JAR=hbase-to-mongo-export-${HBASE_TO_MONGO_EXPORT_VERSION}.jar
COPY --from=buildImage \
        $INSTALL_DIR/build/libs/$JAR \
        $INSTALL_DIR

RUN mkdir certs
COPY resources/certs/htme/* certs/

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}
USER ${SERVICE_USER}
ENTRYPOINT ["sh", "-c", "${INSTALL_DIR}/${JAR} \"$@\"", "--"]
