import os

import pytest
from airflow.models import DagBag

@pytest.fixture
def dag_bag():
    # Получаем список изменённых файлов через переданную переменную CHANGED_FILES
    changed_files = os.getenv("CHANGED_FILES", "").split()
    dag_files = [file for file in changed_files if file.endswith(".py")]  # Фильтруем только .py файлы
    return DagBag(dag_folder=dag_files, include_examples=False)

def test_dagbag_import_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

# Будет писать если не смог найти новые даги (можно будет вырезать эту часть)
def test_dag_count(dag_bag):
    assert len(dag_bag.dags) > 0, "No DAGs found"

def test_dag_has_tasks(dag_bag):
    # Проверка, что DAG имеет хотя бы одну таску
    for dag_id, dag in dag_bag.dags.items():
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"

def test_unique_task_id(dag_bag):
    # Проверка, что все таски (одного DAG) имеют уникальные id
    for dag_id, dag in dag_bag.dags.items():
        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == len(set(task_ids)), f"Duplicate task IDs found in DAG {dag_id}"

def test_dag_has_no_cycles(dag_bag):
    # Проверка, что DAG не содержит циклов (как должно быть по определению)
    for dag_id, dag in dag_bag.dags.items():
        assert dag.test_cycle() is None, f"Cycle detected in DAG {dag_id}"
