import pytest
from airflow.models import DagBag


def test_dag_bag_integrity():
    """
    Test that all DAGs in the Airflow DAGs directory are loadable and have no errors.
    """
    dag_bag = DagBag(dag_folder="airflow/dags", include_examples=False)

    # Check for import errors
    assert (
        len(dag_bag.import_errors) == 0
    ), f"DAG import errors: {dag_bag.import_errors}"

    # Check that we actually found some DAGs
    assert dag_bag.size() > 0, "No DAGs found in airflow/dags"
