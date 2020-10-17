FROM dwp-centos-with-java-htme:latest

EXPOSE 8080
EXPOSE 8443

RUN mkdir -p /opt/data-key-service/data
RUN mkdir -p /opt/data-key-service/logs
WORKDIR /opt/data-key-service

RUN curl --silent --insecure "https://api.github.com/repos/dwp/data-key-service/releases/latest" \
             | jq -r '.assets[0].browser_download_url' > dks_version
RUN cat dks_version
RUN wget --input-file dks_version --no-verbose --no-check-certificate --output-document dks-latest.jar
RUN ls -la *.jar

COPY application_dks.properties ./
COPY dks-keystore.jks ./keystore.jks
COPY dks-truststore.jks ./truststore.jks

RUN chmod a+rwx -R /opt/data-key-service

ENTRYPOINT ["sh", "-c", "java -Dlogging.level.root=DEBUG -Ddks.log.directory=/opt/data-key-service/logs -jar dks-latest.jar \"$@\"", "--"]
