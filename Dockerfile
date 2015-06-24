FROM ubuntu:latest
MAINTAINER Daniel Zohar <daniel@memrise.com>

RUN apt-get update && apt-get install -y libmysqlclient-dev python-dev python-setuptools build-essential libpq-dev
RUN easy_install -U pip
RUN pip install airflow[s3] && pip install airflow[mysql] && pip install airflow[postgres]

ENV AIRFLOW_HOME /airflow
WORKDIR /airflow

EXPOSE 8080
ENTRYPOINT ["airflow"]
CMD ["list_dags"]
