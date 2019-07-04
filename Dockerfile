FROM zenika/kotlin as buildImage

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/${SERVICE_USER}
ENV INSTALL_DIR=/opt/hbase-crown-export
ENV GRADLE='./gradlew --no-daemon'
RUN mkdir -p ${SERVICE_USER_HOME} ${INSTALL_DIR}
RUN useradd -d ${SERVICE_USER_HOME} ${SERVICE_USER}

RUN id -a ${SERVICE_USER}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}
RUN apt-get update
RUN apt-get -q -y install curl zip unzip
USER ${SERVICE_USER}
WORKDIR ${INSTALL_DIR}
RUN curl -s "https://get.sdkman.io" | bash
RUN bash -c ". ${SERVICE_USER_HOME}/.sdkman/bin/sdkman-init.sh && sdk install gradle && $GRADLE wrapper"
RUN ls -l
COPY build.gradle.kts .
COPY settings.gradle.kts .
COPY gradle.properties .
RUN $GRADLE build
