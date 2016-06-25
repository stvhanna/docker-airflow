from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
from fn.func import F
import stringcase as case
import pymongo
import json
import os

now = datetime.utcnow() - timedelta(days=1)
start_date = datetime(now.year, now.month, now.day)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': True,
    'start_date': start_date,
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Pass some env vars through.
env = {
    'AWS_ACCESS_KEY_ID': os.getenv('AWS_ACCESS_KEY_ID', ''),
    'AWS_SECRET_ACCESS_KEY': os.getenv('AWS_SECRET_ACCESS_KEY', ''),
    'AWS_REGION': os.getenv('AWS_REGION', ''),
    'AWS_S3_TEMP_BUCKET': os.getenv('AWS_S3_TEMP_BUCKET', '')
}

# Trim aries-activity- off.
def trim_activity_name(name):
    return name[15:]

def create_task(dag, activity_list, (index, activity)):
    # Get the previous tasks id.
    # This is to mimic original aries functionality.
    # This will probably be replaced with using sensors.
    previous_task = activity_list[index - 1] if index > 0 else None
    previous_task_name = trim_activity_name(previous_task['name']) if previous_task else ''

    # Template out a command.
    command = """
        '{{ task_instance.xcom_pull(task_ids=params.previous_task_name) }}'
        '{{ params.config }}'
        '{{ ts }}'
    """

    # Get config.
    config = json.dumps(activity['config'] if 'config' in activity else {})

    # The params for the command.
    params = { 'config': config, 'previous_task_name': previous_task_name }

    # Get the activity name.
    activity_name = trim_activity_name(activity['name'])

    # Prefix with our docker hub username.
    image_name = 'astronomerio/' + activity_name

    # Return a new docker operator with our command.
    return DockerOperator(
        task_id=activity_name,
        image=image_name,
        environment=env,
        command=command,
        params=params,
        xcom_push=True,
        dag=dag)


# Get mongo url.
mongo_url = os.getenv('MONGO_URL', '')

# Connect to mongo.
client = pymongo.MongoClient(mongo_url)

# Query for all workflows.
for workflow in client.get_default_database().workflows.find({ 'name': { '$exists': True } }):
    # Get the workflow id.
    workflow_id = workflow['_id']

    # Get the name of the workflow.
    name = workflow['name']

    # Legacy.
    interval = timedelta(seconds=int(workflow['interval'])) if 'interval' in workflow else None
    # New.
    schedule = workflow['schedule'] if 'schedule' in workflow else '@daily'
    # Figure out which one to use.
    schedule_interval = schedule if schedule is not None else interval

    # Lower and snake case the name or id.
    formattedName = case.snakecase(case.lowercase(name))

    # Airflow looks at module globals for DAGs, so assign each workflow to
    # a global variable using it's id.
    dag = globals()[workflow_id] = DAG(
        formattedName,
        default_args=default_args,
        schedule_interval=schedule_interval)

    # Grab activity list.
    activity_list = workflow['activityList']

    # List to hold activities to setup dependencies after creation.
    tasks = map(F(create_task, dag, activity_list), enumerate(activity_list))

    # Loop through tasks and set dependencies.
    # TODO: Support `dependsOn` for full blown dags in mongo.
    for i, current in enumerate(tasks):
        if (i > 0): current.set_upstream(tasks[i - 1])
