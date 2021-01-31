FROM python:3.7.3-stretch

ENV SERVICE_USER=dataworks
ENV SERVICE_USER_HOME=/home/$SERVICE_USER
ENV INSTALL_DIR=/opt/aws

RUN mkdir -p $SERVICE_USER_HOME $INSTALL_DIR
RUN useradd -d $SERVICE_USER_HOME $SERVICE_USER

WORKDIR $INSTALL_DIR

COPY ./* ./
RUN ls -al
RUN pip install --trusted-host=pypi.python.org --trusted-host=pypi.org --trusted-host=files.pythonhosted.org -r ./requirements.txt
RUN chown -R $SERVICE_USER.$SERVICE_USER $SERVICE_USER_HOME
RUN chown -R $SERVICE_USER.$SERVICE_USER $INSTALL_DIR

USER $SERVICE_USER
ENTRYPOINT ["./run.sh"]
