"""
FTP content ingestion via S3 bucket wildcard key into Airflow.
"""

import datetime
import os

from airflow import DAG
from airflow.operators import (
    AstronomerS3GetKeyAction, AstronomerS3KeySensor, DummyOperator,
)
from fn.func import F
import pymongo
import stringcase

from util.docker import create_linked_docker_operator

MONGO_URL = os.getenv('MONGO_URL', '')
S3_BUCKET = 'astronomer-ftp'  # TODO: put this in an env var in infra

now = datetime.datetime.utcnow() - datetime.timedelta(hours=1)
start_date = datetime.datetime(now.year, now.month, now.day, now.hour)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
    'start_date': start_date,
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
}

client = pymongo.MongoClient(MONGO_URL)

ftp_configs = client.get_default_database().ftpConfigs.find({})
print('Found {} ftp_configs.'.format(ftp_configs.count()))

# one FTP config per workflow and each customer can have zero or more workflows
for ftp_config in ftp_configs:
    id_ = ftp_config['_id']
    config_name = stringcase.snakecase(ftp_config['name'].lower())
    path = ftp_config['path']
    schedule = ftp_config['schedule']
    poke_interval = int(ftp_config['pokeInterval'])
    timeout = int(ftp_config['timeout'])
    activity_list = ftp_config['activityList']

    dag_name = '{config_name}__ftp__{id}'.format(config_name=config_name, id=id_)
    print('Building DAG', dag_name)

    dag = DAG(dag_name, default_args=default_args, schedule_interval=schedule)
    globals()[id_] = dag  # TODO: may be unnecessary

    op_0_dummy = DummyOperator(task_id='start', dag=dag)

    # probe for files
    task_1_s3_sensor = AstronomerS3KeySensor(
        task_id='s3_ftp_config_sensor',
        bucket_name=S3_BUCKET,
        bucket_key=path,
        soft_fail=True,
        poke_interval=poke_interval,
        timeout=timeout,
        dag=dag,
    )
    task_1_s3_sensor.set_upstream(op_0_dummy)

    # grab files
    task_2_s3_get = AstronomerS3GetKeyAction(
        task_id='s3_ftp_config_get_key',
        bucket_name=S3_BUCKET,
        bucket_key=path,
        soft_fail=True,
        poke_interval=poke_interval,
        timeout=timeout,
        dag=dag,
    )
    task_2_s3_get.set_upstream(task_1_s3_sensor)

    # schedule downstream activity dependencies
    tasks = map(
        F(create_linked_docker_operator, dag, activity_list, ''),
        enumerate(activity_list),
    )
    for i, current in enumerate(tasks):
        if (i == 0):
            current.set_upstream(task_2_s3_get)
        else:
            current.set_upstream(tasks[i - 1])

    # TODO: archive file on S3 (see my notes)

client.close()
