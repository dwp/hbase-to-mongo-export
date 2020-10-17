FROM centos:latest

RUN mkdir -p /opt/hbase-to-mongo-export/data

RUN yum -y upgrade
RUN yum install -y epel-release
RUN yum install -y java-1.8.0-openjdk wget jq
ENV JAVA_HOME /etc/alternatives/jre

RUN chmod a+rwx -R /opt/hbase-to-mongo-export/ /opt/hbase-to-mongo-export/data
