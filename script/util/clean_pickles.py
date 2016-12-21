# This is used to clear out old dag_pickles to prevent our postgres
# volume from running out of space with unnecessary pickles.

# Airflow 1.7.x requires DAG to be imported to run the file.
from airflow import DAG, models, settings
from datetime import datetime, timedelta

print('Deleting old pickles...')

session = settings.Session()
session.query(models.DagPickle).filter(models.DagPickle.created_dttm < datetime.now()-timedelta(hours=12)).delete()
session.commit()
session.close()

print('Deleted old pickles!')
