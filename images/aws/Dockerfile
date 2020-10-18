FROM python:3-buster

COPY environment.sh ./
COPY run.sh ./

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}
USER $SERVICE_USER

ENTRYPOINT ["./run.sh"]
