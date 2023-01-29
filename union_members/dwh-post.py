import logging
import requests as r
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable, Connection

from datetime import datetime, timedelta

@task(task_id='post_data', retries=3)
def post_data():
    con = Connection.get_connection_from_secrets('postgres_dwh').get_uri()
    query = """
    SELECT last_name, profcom_id FROM STG_UNION_MEMBER.union_member
    WHERE(faculty='Физический факультет') AND(status='Члены Профсоюза')
    """

    data = pd.read_sql_query(query, con.replace("postgres://", "postgresql://"))
    data.to_sql(
        'union_member',
        Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://"),
        schema='STG_UNION_MEMBER',
        index=False,
    )