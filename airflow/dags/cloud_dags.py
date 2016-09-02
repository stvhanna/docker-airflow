from airflow import DAG
from airflow.operators.docker_operator import DockerOperator
from datetime import datetime, timedelta
from fn.func import F
import ast
import stringcase as case
import pymongo
import json
import os

now = datetime.utcnow() - timedelta(hours=1)
start_date = datetime(now.year, now.month, now.day, now.hour)

default_args = {
    'owner': 'astronomer',
    'depends_on_past': False,
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
    previous_task_id = (
            '{index}_{name}'.format(index=index-1, name=trim_activity_name(previous_task['name'])) if previous_task
            else '')

    # Template out a command.
    command = """
        '{{ task_instance.xcom_pull(task_ids=params.previous_task_id) }}'
        '{{ params.config }}'
        '{{ ts }}'
    """

    # Get config.
    config = json.dumps(activity['config'] if 'config' in activity else {})

    # The params for the command.
    params = { 'config': config, 'previous_task_id': previous_task_id }

    # Get the activity name.
    activity_name = trim_activity_name(activity['name'])

    # Create task id.
    task_id = '{index}_{name}'.format(
            index=index,
            name=trim_activity_name(activity['name']))

    # Prefix with our docker hub username.
    image_name = 'astronomerio/' + activity_name

    # Force pull in prod, use local in dev.
    force_pull = ast.literal_eval(os.getenv('FORCE_PULL_TASK_IMAGES', 'True'))

    # Return a new docker operator with our command.
    return DockerOperator(
        task_id=task_id,
        image=image_name,
        environment=env,
        command=command,
        params=params,
        xcom_push=True,
        force_pull=force_pull,
        dag=dag)


# Get mongo url.
mongo_url = os.getenv('MONGO_URL', '')

# Connect to mongo.
print('Connecting to mongodb.')
client = pymongo.MongoClient(mongo_url)

# Query for all workflows.
print('Querying for cloud workflows.')
workflows = client.get_default_database().workflows.find({ '_airflow': True })

print('Found {count} workflows.'.format(count=workflows.count()))
for workflow in workflows:
    # Get the workflow id.
    workflow_id = workflow['_id']

    # Get the name of the workflow.
    workflow_name = workflow['name'] if 'name' in workflow else None

    # Lower and snake case the name if we have one, else just id.
    name = workflow_id if not workflow_name else '{name}__etl__{id}'.format(
        id=workflow_id,
        name=case.snakecase(case.lowercase(workflow_name)))

    print('Building DAG: {name}.').format(name=name)

    # Legacy.
    interval = timedelta(seconds=int(workflow['interval'])) if 'interval' in workflow else None
    # New.
    schedule = workflow['schedule'] if 'schedule' in workflow else None
    # Figure out which one to use.
    schedule_interval = schedule if schedule is not None else interval

    # Airflow looks at module globals for DAGs, so assign each workflow to
    # a global variable using it's id.
    dag = globals()[workflow_id] = DAG(
        name,
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

client.close()
print('Finished exporting ETL DAG\'s.')
