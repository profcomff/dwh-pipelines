import logging
import requests as r
import pandas as pd
from airflow.decorators import dag, task
from airflow.models import Variable, Connection


from datetime import datetime, timedelta


@dag(schedule='@daily', start_date=datetime(2023, 1, 1, 2, 0, 0), catchup=False)
def generate_union_member_sync():
    @task(task_id='fetch_users', retries=3)
    def fetch_union_members():
        """Скачать данные из ЛК ОПК"""

        with r.Session() as s:
            logging.info("Using user %s to fetch", Variable.get("LK_MSUPROF_ADMIN_USERNAME"))

            s.headers = {
                "referer": "https://lk.msuprof.com/adminlogin/?next=/admin",
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/102.0.0.0 Safari/537.36',
                'accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9'
            }

            resp = s.get(
                "https://lk.msuprof.com/adminlogin/?next=/admin",
            )
            logging.info(resp)

            resp=s.post(
                "https://lk.msuprof.com/adminlogin/?next=/admin",
                data={
                    "csrfmiddlewaretoken": s.cookies['csrftoken'],
                    "username": str(Variable.get("LK_MSUPROF_ADMIN_USERNAME")),
                    "password": str(Variable.get("LK_MSUPROF_ADMIN_PASSWORD")),
                    "next": "/admin",
                },
            )
            logging.info(resp)

            resp = s.post(
                "https://lk.msuprof.com/get-table/",
                data={
                    "current-role": "Администратор",
                    "f_role": "",
                    "f_status": "",
                    "page-status": "user",
                },
            )
            logging.info(resp)

        try:
            users_dict = resp.json()["data"]
        except Exception as e:
            logging.error("Failed to fetch data from lk.msuprof.com")
            raise e

        pd.DataFrame(users_dict).to_sql(
            'union_member',
            Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://"),
            schema='STG_UNION_MEMBER',
            if_exists='replace',
            index=False,
        )

    fetch_union_members()


union_member_sync = generate_union_member_sync()
