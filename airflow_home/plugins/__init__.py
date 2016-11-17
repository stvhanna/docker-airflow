from airflow.plugins_manager import AirflowPlugin
from plugins import operators


class AstronomerPlugin(AirflowPlugin):
    name = "astronomer_plugin"
    operators = [
        operators.AstronomerS3GetKeyAction,
        operators.AstronomerS3KeySensor
    ]
