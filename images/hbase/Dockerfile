FROM python:3-buster

RUN pip install awscli --trusted-host=pypi.org --trusted-host=pypi.python.org --trusted-host=files.pythonhosted.org

COPY ./* ./
RUN pip3 install --trusted-host=pypi.python.org --trusted-host=pypi.org --trusted-host=files.pythonhosted.org -r ./requirements.txt
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${SERVICE_USER_HOME}
RUN chown -R ${SERVICE_USER}.${SERVICE_USER} ${INSTALL_DIR} ${INSTALL_DIR}/data
RUN ls -l ${INSTALL_DIR}/data

USER $SERVICE_USER
ENTRYPOINT ["./hbase_data.py"]
