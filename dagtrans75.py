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
        conn.execute(
        '''
        with sq as (select
  id,
  record->>'message' as e_msg,
  container_name,
  create_ts
from
  "ODS_INFRA_LOGS".container_log
where
  record->>'level' = 'ERROR' or record->>'level' = 'CRITICAL') 
        merge into "DM_INFRA_LOGS".incident_hint as ne 
        using sq as e on
        (ne.container_name = e.container_name) and (ne.message = e.e_msg) and (ne.create_ts = e.create_ts)
        when not matched then 
        insert (id,msk_record_loaded_dttm,container_name,message,create_ts)
        values (e.id,now(),e.container_name,e.e_msg,e.create_ts)''')
        conn.commit()
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
        
        
            
