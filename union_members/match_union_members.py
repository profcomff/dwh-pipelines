from datetime import datetime
from textwrap import dedent

from airflow import DAG, Dataset
from airflow.providers.postgres.operators.postgres import PostgresOperator

with DAG(
    dag_id="union_members_match",
    start_date=datetime(2024, 1, 1),
    schedule=[Dataset("ODS.user_info", "STG.Union_member.Union_member")],
    catchup=False,
    tags=["stg", "ods", "matching", "union_members"],
    default_args={"owner": "SofyaFin"},
) as dag:

    insert_union_members = PostgresOperator(
        task_id="insert_union_members",
        postgres_conn_id="postgres_dwh",
        sql=dedent("""\
            INSERT INTO ODS.User.union_member (fio, card_number, card_date, card_status) 
            SELECT 
                a.full_name, 
                a.student_id_number, 
                b.card_date, 
                b.card_status 
            FROM 
                ODS.user_info AS a 
            LEFT JOIN 
                STG.Union_member.Union_member AS b 
            ON 
                (a.full_name = CONCAT(b.first_name, ' ', b.middle_name, ' ', b.last_name)) 
                AND (a.student_id_number = b.card_number);
        """),
        inlets=[Dataset("ODS.user_info", "STG.Union_member.Union_member")],
        outlets=[Dataset("ODS.User.union_member")],
    )

    check_column_count = PostgresOperator(
        task_id="check_column_count",
        postgres_conn_id="postgres_dwh",
        sql=dedent("""\
            WITH 
                count_ods AS (
                    SELECT COUNT(*) AS column_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'union_member' AND TABLE_SCHEMA = 'ODS.User'
                ),
                count_stg AS (
                    SELECT COUNT(*) AS column_count
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_NAME = 'Union_member' AND TABLE_SCHEMA = 'STG.Union_member'
                )
            SELECT 
                CASE 
                    WHEN ods.column_count = stg.column_count THEN 'Количество колонок совпадает: ' || ods.column_count
                    ELSE 'Количество колонок не совпадает: ODS.User.union_member имеет ' || ods.column_count || 
                         ' колонок, а STG.Union_member.Union_member имеет ' || stg.column_count || ' колонок.'
                END AS result
            FROM count_ods ods, count_stg stg;
        """),
    )

    insert_union_members >> check_column_count  # Set task dependency

