from airflow import DAG, models, settings
from datetime import datetime, timedelta
from airflow.operators.bash_operator import BashOperator


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2015, 6, 1),
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

print('Cleaning old pickles') 
 
session = settings.Session() 
old_pickles = session.query(models.DagPickle).filter(models.DagPickle.created_dttm < datetime.now()-timedelta(hours=12)).all() 
for p in old_pickles: 
    session.delete(p) 
 
session.commit() 
session.close() 

print('Pickles clean')