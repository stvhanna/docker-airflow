FROM ubuntu:latest
MAINTAINER schnie <greg@astronomer.io>

RUN apt-get update && apt-get install -y python2.7-dev python-pip python-setuptools build-essential libpq-dev
RUN pip install airflow==1.7.0

ENV AIRFLOW_HOME /airflow
WORKDIR /airflow

EXPOSE 8080
ENTRYPOINT ["airflow"]
CMD ["list_dags"]
