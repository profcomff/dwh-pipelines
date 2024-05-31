from sqlalchemy import create_engine
import sqlalchemy as sa
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from sqlalchemy import text
from datetime import datetime, timedelta

@task(task_id="trans_from_ods_to_dm_infralogs")
def trans():
    dwhuri = (
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
    )
    dwh_sql_engine = create_engine(dwhuri) #создаем движок
    with dwh_sql_engine.connect() as conn:
        res = conn.execute(sa.text(
        f'''select
        id,
        record->>'message',
        container_name,
        create_ts
        from
        ODS_INFRA_LOGS.container_log
        where
        record->>'level_name' = 'ERROR' or record->>'level_name' = 'CRITICAL';''')).fetchall()
        id = []
        message = []
        container_name = []
        create_ts = []
        for i in range(len(res)):
            id[i] = res[i][0]
            message[i] = res[i][1]
            container_name[i] =res[i][2]
            create_ts[i] = res[i][3]
            conn.execute(
            '''
            insert into DM_INFRA_LOGS.incident(id,message,container_name,create_ts) VALUES ({id[i]},{message[i]},{container_name[i]},{create_ts[i]})''')
        conn.execute(
        '''
        merge into  ODS_INFRA_LOGS.container_log as e,
        using DM_INFRA_LOGS.incident as ne,
        on e.id = ne.id
        when matched then 
        update set (e.create_ts = ne.create_ts) and (e.message = ne.message) and (e.container_name = ne.container_name) and (e.id = ne.id)
        when not matches then 
        insert into infra_logs_Incident values (e.id, e.message, e.container_name, e.create_ts)''')
@dag(
    schedule='0 */1 * * *',
    start_date=datetime(2024, 1, 1, 2, 0, 0),
    catchup=False,
    tags=["dwh"],
    default_args={"owner": "dwh", "retries": 3, "retry_delay": timedelta(minutes=5)}
    
)

def start():
      trans()

start()
        
        
            
