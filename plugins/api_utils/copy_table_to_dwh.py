import logging

import pandas as pd
from airflow.models import Connection
from sqlalchemy import create_engine


def copy_rating_to_rating_api():
    API_DB_DSN = (
        Connection.get_connection_from_secrets("postgres_api")
        .get_uri()
        .replace("postgres://", "postgresql://")
        .replace("?__extra__=%7B%7D", "")
    )

    DWH_DB_DSN = (
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
        .replace("?__extra__=%7B%7D", "")
    )

    api = create_engine(API_DB_DSN)
    dwh = create_engine(DWH_DB_DSN)

    logging.info("Перенос рейтинга преподавателя из DWH_RATING в rating_api")
    with api.connect() as api_conn, dwh.connect() as dwh_conn:
        data = pd.read_sql_query(
            f'SELECT api_id, mark_weighted, mark_kindness_weighted, mark_clarity_weighted, mark_freebie_weighted, rank '
            f'FROM "DWH_RATING".lecturer as lect '
            f'WHERE lect.valid_to_dt is NULL and lect.valid_from_dt = ('
            f'SELECT MAX(valid_from_dt) FROM "DWH_RATING".lecturer)',
            dwh_conn,
            index_col="api_id",
        )

        api_conn.execute('TRUNCATE TABLE lecturer_rating;')
        data.to_sql(name="lecturer_rating", con=api_conn, if_exists="append")
        logging.info("Данные рейтинга преподавателей были скопированы с dwh в rating_api")
