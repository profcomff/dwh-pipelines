from datetime import datetime, timedelta
from functools import partial

from airflow import DAG
from airflow.models import Variable

from plugins.features import alert_message


base_url = "https://github.com/profcomff/dwh-pipelines/blob/main/"


class DAG_TEMPLATE(DAG):

    def __init__(self, dag_id: str, path: str, **kwargs):
        get_chat_id = lambda: int(Variable.get("TG_CHAT_DWH"))

        default_dag_args = {
            'catchup': False,
            'doc_md': base_url + path,
            'on_failure_callback': lambda ctx: alert_message(chat_id=get_chat_id(), context=ctx),
            **kwargs,
        }

        self.default_task_args = {
            'retries': 3,
            'retry_delay': timedelta(minutes=1),
            'retry_exponential_backoff': True,
            'max_retry_delay': timedelta(minutes=25),
            'execution_timeout': timedelta(minutes=30),
        }

        super().__init__(dag_id=dag_id, default_args=self.default_task_args, **default_dag_args)
