FROM zenika/kotlin as buildImage




ENV SERVICE_USER=dataworks
RUN mkdir /home/${SERVICE_USER}
RUN useradd  -d /home/${SERVICE_USER} ${SERVICE_USER}
RUN id -a ${SERVICE_USER}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} /home/${SERVICE_USER}
RUN apt-get update
RUN apt-get -q -y install curl zip unzip
USER ${SERVICE_USER}
RUN curl -s "https://get.sdkman.io" | bash
RUN exec bash
RUN . /home/${SERVICE_USER}/.sdkman/bin/sdkman-init.sh
RUN sdk install gradle
