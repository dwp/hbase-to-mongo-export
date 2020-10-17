FROM dwp-python-preinstall-htme:latest

COPY environment.sh ./
COPY run.sh ./

RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR}
USER $SERVICE_USER

ENTRYPOINT ["./run.sh"]
