import json
import logging
from typing import Set, Tuple

import pandas as pd
import sqlalchemy as sa
from airflow.models import Connection
from sqlalchemy import create_engine, text


MAX_ROWS_PER_REQUEST = 10_000

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

__COPY_CTX_ENCRYPT_COLS = None

__COPY_CTX_KEY_TABLE = "STG_USERDATA.keys"

__COPT_CTX_KEY_OWNER_COL = None


def copy_table_to_dwh(
    from_schema,
    from_table,
    to_schema,
    to_table,
    encrypt_cols=None,
    key_owner_column=None,
    key_table="STG_USERDATA.keys",
):
    logging.info(f"Копирование таблицы {from_schema}.{from_table} в {to_schema}.{to_table}")

    api = create_engine(API_DB_DSN)
    dwh = create_engine(DWH_DB_DSN)

    with api.connect() as api_conn:
        api_cols_all = api_conn.execute(
            sa.text(
                "SELECT column_name, data_type, ordinal_position "
                "FROM information_schema.columns "
                f"WHERE table_schema = '{from_schema}' AND table_name = '{from_table}';"
            )
        )

        bad_api_cols = set()
        api_cols = set()
        id_column = None
        for name, dtype, pos in api_cols_all:
            if pos == 1:
                id_column = str(name)
            if dtype == "json":
                bad_api_cols.add((str(name), dtype))
            api_cols.add(str(name))
        logging.info(f"Колонки в источнике: {api_cols}")
        logging.info(f"Колонки с особенностями: {bad_api_cols}")

        if "id" in api_cols:
            id_column = "id"
        elif "create_ts" in api_cols:
            id_column = "create_ts"
        elif id_column is None:
            raise AttributeError("Не найдена колонка для сортировки")
        logging.info(f"Колонка для сортировки: {id_column}")

        data_length = api_conn.execute(sa.text(f'SELECT COUNT(*) FROM "{from_schema}"."{from_table}";')).scalar()
        logging.info(f"Количество строк: {data_length}")

    with dwh.connect() as dwh_conn:
        dwh_cols = dwh_conn.execute(
            sa.text(
                "SELECT column_name "
                "FROM information_schema.columns "
                f"WHERE table_schema = '{to_schema}' AND table_name = '{to_table}';"
            )
        )
        dwh_cols = set(str(i[0]) for i in dwh_cols)
        logging.info(f"Колонки в таргете: {dwh_cols}")

        dwh_conn.execute(sa.text(f'TRUNCATE TABLE "{to_schema}"."{to_table}";'))

    to_copy_cols = api_cols & dwh_cols
    logging.info(f"Колонки для копирования: {to_copy_cols}")

    with api.connect() as api_conn, dwh.connect() as dwh_conn:
        to_copy_cols = '", "'.join(to_copy_cols)
        for i in range(0, data_length, MAX_ROWS_PER_REQUEST):
            data = pd.read_sql_query(
                f'SELECT "{to_copy_cols}" '
                f'FROM "{from_schema}"."{from_table}" '
                f"ORDER BY {id_column} "
                f"LIMIT {MAX_ROWS_PER_REQUEST} OFFSET {i}",
                api_conn,
                id_column,
            )
            for col, dtype in bad_api_cols:
                # Делаем ручное приведение типов, где не сработало иное
                # Заменяем JSON на строку
                if dtype == "json":
                    data[col] = data[col].apply(lambda x: json.dumps(x, ensure_ascii=False))
            if encrypt_cols is None:
                data.to_sql(to_table, dwh, schema=to_schema, if_exists="append")
            else:
                __COPY_CTX_ENCRYPT_COLS = set(encrypt_cols)
                __COPY_CTX_KEY_OWNER_COL = key_owner_column
                __COPY_CTX_KEY_TABLE = key_table
                data.to_sql(to_table, dwh, schema=to_schema, if_exists="append", method=__custom_execute)
                logging.info(f"encrypting columns {encrypt_cols}")
            logging.info("%d of %d rows copied", i + len(data), data_length)


# нужна чисто как callback в pandas.DataFrame.to_sql. Т.к. разрабы pandas решили не париться с тем,
# чтобы в callback можно было передать доп. аргументы, они передаются через глобальные переменные
# __COPY_CTX_*
def __custom_execute(pd_table, conn, keys, data_iter):
    data_iter = [{key: val for key, val in zip(keys, data)} for data in data_iter]
    collist = [
        (
            f":{key}"
            if key not in __COPY_CTX_ENCRYPT_COLS
            else f"pgp_sym_encrypt(:{key}, {__COPY_CTX_KEY_TABLE}.key::text)"
        )
        for key in keys
    ]
    # TODO: проверить, что у KEY_TABLE.id = data_iter[__COPY_CTX_KEY_OWNER_COL][i] РОВНО 1 СОВПАДЕНИЕ
    # если оно не 1, то закричать о помощи
    sql = text(
        f"INSERT INTO {pd_table.name}({','.join(keys)}) "
        f"SELECT {','.join(collist)} FROM {__COPY_CTX_KEY_TABLE} "
        f"WHERE {__COPY_CTX_KEY_TABLE}.id = :{__COPY_CTX_KEY_OWNER_COL}"
        "ON CONFLICT (id) DO NOTHING;"
    )
    exec_result = conn.execute(sql, data_iter)
