FROM ubuntu:15.04
MAINTAINER schnie <greg@astronomer.io>

RUN apt-key adv --keyserver keyserver.ubuntu.com --recv E56151BF
RUN echo "deb http://repos.mesosphere.com/ubuntu vivid main" | tee /etc/apt/sources.list.d/mesosphere.list
RUN apt-get update && apt-get install -y mesos supervisor python-dev python-setuptools build-essential libpq-dev dnsutils netcat
RUN easy_install pip
RUN pip install protobuf psycopg2 airflow==1.7.1.3

ENV PYTHONPATH=${PYTHONPATH}:/usr/lib/python2.7/site-packages/

COPY config /etc/supervisor/conf.d/

WORKDIR /astronomer
COPY astronomer /astronomer
RUN pip install -r requirements.txt

ENV AIRFLOW_HOME /airflow
WORKDIR /airflow
COPY airflow/airflow.cfg /airflow/
COPY entrypoint.sh /airflow/

EXPOSE 8080 5555 8793

ENTRYPOINT ["./entrypoint.sh"]
