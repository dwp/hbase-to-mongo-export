FROM zenika/kotlin as buildImage

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/${SERVICE_USER}
ENV INSTALL_DIR=/opt/hbase-crown-export
ENV GRADLE='./gradlew --no-daemon'

RUN mkdir -p ${SERVICE_USER_HOME} ${INSTALL_DIR}
RUN useradd -d ${SERVICE_USER_HOME} ${SERVICE_USER}
RUN id -a ${SERVICE_USER}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}

WORKDIR ${INSTALL_DIR}

COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
COPY gradlew .
COPY gradle/ ./gradle

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}

USER ${SERVICE_USER}

RUN $GRADLE wrapper
RUN $GRADLE --refresh-dependencies compileKotlin
RUN find ${SERVICE_USER_HOME}/.gradle -type f
