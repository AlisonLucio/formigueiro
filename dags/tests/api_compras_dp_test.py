import pytest

from airflow.models import DagBag


@pytest.fixture()
def dagbag():
    return DagBag()


def test_dag_loaded(dagbag):
    dag = dagbag.get_dag(dag_id='apis_gov_br_batch_dataproc')
    assert dagbag.import_errors == {}
    assert dag is not None
    assert len(dag.tasks) == 1