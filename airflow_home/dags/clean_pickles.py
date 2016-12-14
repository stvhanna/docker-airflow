# Airflow 1.7.x requires DAG to be imported to run the file.
from airflow import DAG, models, settings
from datetime import datetime, timedelta

print('Cleaning old pickles')

session = settings.Session()
old_pickles = session.query(models.DagPickle).filter(models.DagPickle.created_dttm < datetime.now()-timedelta(hours=12)).all()
for p in old_pickles:
    session.delete(p)

session.commit()
session.close()

print('Pickles cleaned')
