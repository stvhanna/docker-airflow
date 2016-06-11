from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
from fn.func import F
import stringcase as case
import pymongo
import json

default_args = {
    'owner': 'schnie',
    'depends_on_past': True,
    'start_date': datetime(2016, 5, 4),
    'email': 'greg@astronomer.io',
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

env = {
    'AWS_ACCESS_KEY_ID': 'AKIAILGTIA5SRLTJTXWQ',
    'AWS_SECRET_ACCESS_KEY': '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',
    'AWS_REGION': 'us-east-1',
    'AWS_S3_TEMP_BUCKET': 'astronomer-workflows',
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

    # Dummy activityTask
    activity_task = '{}'

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


# Connect to mongo.
client = pymongo.MongoClient('localhost', 3001)

# Query for all workflows.
# XXX: REMOVE ME!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!
for workflow in client.meteor.workflows.find({ '_id': 'TNWMS3WjdSuLtXqEr' }):
    # Get the workflow id.
    workflow_id = workflow['_id']

    # Get the name of the workflow, fallback to id if missing.
    name = workflow['name'] if 'name' in workflow else workflow_id

    # Lower and snake case the name or id.
    formattedName = case.snakecase(case.lowercase(name))

    # Airflow looks at module globals for DAGs, so assign each workflow to
    # a global variable using it's id.
    dag = globals()[workflow_id] = DAG(
        formattedName,
        default_args=default_args,
        schedule_interval=timedelta(1))

    activity_list = workflow['activityList']

    # List to hold activities to setup dependencies after creation.
    tasks = map(F(create_task, dag, activity_list), enumerate(activity_list))

    # Loop through tasks and set dependencies.
    # TODO: Support `dependsOn` for full blown dags in mongo.
    for i, current in enumerate(tasks):
        if (i > 0): current.set_upstream(tasks[i - 1])
