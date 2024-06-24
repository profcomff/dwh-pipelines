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
        merge into  ODS_INFRA_LOGS.container_log as e,
        using with (select * from log where not is_deleted) as sq select * from sq,
        on e.message = ne.message 
        when not matches then 
        insert into infra_logs_Incident values sq''')
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
        
        
            
