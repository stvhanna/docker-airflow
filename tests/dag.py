import unittest
from airflow_home.dags.util.dag import *

class TestDag(unittest.TestCase):

    def test_dependsOn(self):
        tasks = [""]
        actual = build_dag(tasks)
        assert len(actual) is len(tasks)

if __name__ == '__main__':
    unittest.main()
