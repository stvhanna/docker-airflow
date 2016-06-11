FROM ubuntu:latest
MAINTAINER schnie <greg@astronomer.io>

RUN apt-get update && apt-get install -y python2.7 python-pip python-setuptools build-essential libpq-dev
RUN pip install airflow==1.7.0

WORKDIR /astronomer
COPY astronomer /astronomer
RUN pip install -r requirements.txt

ENV AIRFLOW_HOME /airflow
WORKDIR /airflow
COPY airflow/airflow.cfg /airflow/

EXPOSE 8080
ENTRYPOINT ["airflow"]
CMD ["list_dags"]
