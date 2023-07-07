import pytest
from helpers.apis.dicts.db_postgresql.base import DAG_CONFIG

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id=DAG_CONFIG)
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1