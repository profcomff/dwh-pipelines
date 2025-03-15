import pytest
from airflow.models import DagBag
import os

# функция поиска .py файлов по директории
def find_dag_files(directory):
    dag_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(".py"):
                dag_files.append(os.path.join(root, file))
    return dag_files

# фикстура загрузки всех .py файлов
@pytest.fixture
def dag_bag():
    dag_files = find_dag_files(".")
    return DagBag(dag_folder=dag_files, include_examples=False)

def test_dagbag_import_errors(dag_bag):
    assert len(dag_bag.import_errors) == 0, f"DAG import errors: {dag_bag.import_errors}"

def test_dag_count(dag_bag):
    assert len(dag_bag.dags) > 0, "No DAGs found"

def test_dag_structure(dag_bag):
    for dag_id, dag in dag_bag.dags.items():
        # Проверка, что DAG имеет хотя бы одну задачу
        assert len(dag.tasks) > 0, f"DAG {dag_id} has no tasks"

        # Проверка, что все задачи имеют уникальные id
        task_ids = [task.task_id for task in dag.tasks]
        assert len(task_ids) == len(set(task_ids)), f"Duplicate task IDs found in DAG {dag_id}"

        # Проверка, что DAG не содержит циклов
        assert dag.test_cycle() is None, f"Cycle detected in DAG {dag_id}"