from datetime import datetime, timedelta
from textwrap import dedent
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow import DAG
from airflow.datasets import Dataset

with DAG(
    dag_id="DM_USER.unionmembers_join_with_users",
    schedule=[Dataset("DWH_USER_INFO.info"), Dataset("STG_UNION_MEMBER.union_member")],
    start_date=datetime(2024, 8, 27),
    tags=["dm", "src", "userdata"],
    default_args={
        "owner": "wudext",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    PostgresOperator(
        task_id="make_join",
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            """
            INSERT INTO "DM_USER".union_member_join
            select 
            userdata.user_id as userdata_user_id,
            userdata.full_name as full_name,
            um.first_name as first_name,
            um.last_name as last_name,
            um.type_of_learning as type_of_learning,
            um.rzd_status as wtf_value,
            um.academic_level as academic_level,
            um.rzd_number as rzd_number,
            um.card_id as card_id,
            um.card_number as card_number
            from 
            (SELECT 
            STRING_TO_ARRAY(email, ',') as email_list,
            user_id,
            full_name
            FROM "DWH_USER_INFO".info
            ) as userdata
            join "STG_UNION_MEMBER".union_member as um
            on um.email=any(userdata.email_list)
            ON CONFLICT (full_name)
            DO UPDATE SET
                userdata_user_id = EXCLUDED.userdata_user_id,
                full_name = EXCLUDED.full_name,
                first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                type_of_learning = EXCLUDED.type_of_learning,
                wtf_value = EXCLUDED.wtf_value,
                academic_level = EXCLUDED.academic_level,
                rzd_number = EXCLUDED.rzd_number,
                card_id = EXCLUDED.card_id,
                card_number = EXCLUDED.card_number;
            """,
        ),
        inlets=[
            Dataset("DWH_USER_INFO.info"),
            Dataset("STG_UNION_MEMBER.union_member"),
        ],
        outlets=[Dataset("DM_USER.union_member_join")],
    )

with DAG(
    dag_id="DM_USER.unionmembers_join_with_users_id_only",
    schedule=[Dataset("DWH_USER_INFO.info"), Dataset("STG_UNION_MEMBER.union_member")],
    start_date=datetime(2025, 2, 27),
    tags=["dm", "src", "userdata"],
    default_args={
        "owner": "timofeevna",
        "retries": 3,
        "retry_delay": timedelta(minutes=5),
    },
) as dag:
    PostgresOperator(
        task_id="make_join",
        postgres_conn_id="postgres_dwh",
        sql=dedent(
            """
            INSERT INTO "DM_USER".union_member_card
            select 
            userdata.user_id as userdata_user_id,
            um.card_id as card_id,
            um.card_number as card_number
            from 
            (SELECT 
            STRING_TO_ARRAY(email, ',') as email_list,
            user_id
            FROM "DWH_USER_INFO".info
            ) as userdata
            join "STG_UNION_MEMBER".union_member as um
            on um.email=any(userdata.email_list)
            """,
        ),
        inlets=[
            Dataset("DWH_USER_INFO.info"),
            Dataset("STG_UNION_MEMBER.union_member"),
        ],
        outlets=[Dataset("DM_USER.union_member_card")],
    )