from airflow.plugins_manager import AirflowPlugin

from plugins import operators
from plugins import executors

class AstronomerPlugin(AirflowPlugin):
    name = "astronomer_plugin"
    operators = [
        operators.AstronomerS3KeySensor
    ]
    executors = [
        executors.AstronomerMesosExecutor
    ]
