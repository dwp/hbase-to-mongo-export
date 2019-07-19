FROM python:3.7.3-stretch

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/${SERVICE_USER}
ENV INSTALL_DIR=/opt/hbase-to-mongo-export
RUN mkdir -p ${SERVICE_USER_HOME} ${INSTALL_DIR}/data
RUN useradd -d ${SERVICE_USER_HOME} ${SERVICE_USER}
RUN id -a ${SERVICE_USER}

WORKDIR $INSTALL_DIR
COPY requirements.txt *.py sample-data.json ./
RUN pip3 install -r ./requirements.txt

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}
USER $SERVICE_USER
ENTRYPOINT ["./populate_hbase.py", "./sample-data.json"]
