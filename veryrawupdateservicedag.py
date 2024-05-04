from airflow.decorators import task
from airflow.models import Connection, create_engine
from sqlalchemy import Base, Mapped
@task(task_id="update_service")
class ods_timetable_act(Base):
    key: Mapped[int] = mapped_column(Integer, Nullable = None, primary_key = True)
    event_text: Mapped[str] = mapped_column(String, Nullable = None)
    time_interval_text: Mapped[str] = mapped_column(String, Nullable = None)
    group_text: Mapped[str] = mapped_column(String, Nullable = None)
prinadl = [{"DWH":"ods_timetable_act"}]
schemas = [{"event_text"},{"time_interval_text"},{"up_text"}]
input(schemas)
def create_sv():
dwhuri = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
)
dwh_sql_engine = create_engine(dwhuri)  # создаем движок
with dwh_sql_engine.connect() as conn:
    e = len(schemas[1])
    key = 0
    for u in range(e):
        key = key+1
        conn.execute(f'''CREATE TABLE {prinadl[u]["DWH"]} (
       {key} INTEGER,
       {event_text} TEXT,
       {time_interval_text} TEXT NOT NULL,
       {group_text} TEXT NOT NULL
        PRIMARY KEY {key});''')
        conn.commit()
def update_sv():
    new_schemas = [{"nevent_text"}, {"ntime_interval_text"}, {"ngroup_text"}]
    input(new_schemas)
    dwhuri = (
        Connection.get_connection_from_secrets("postgres_dwh")
        .get_uri()
        .replace("postgres://", "postgresql://")
    )
    dwh_sql_engine = create_engine(dwhuri)  # создаем движок
    with (dwh_sql_engine.connect() as conn):
        e = len(schemas[1])
        for t in range(e):
            conn.execute(f'''DROP TABLE {prinadl[t]["DWH"]} CASCADE''')
            conn.commit()
        r = len(new_schemas)
        key = 0
        for h in range(r):
            key = key + 1
            conn.execute(f'''CREATE TABLE {prinadl[h]["DWH"]} (
                {key} INTEGER,
                {new_event_text} TEXT,
                {new_time_interval_text} TEXT NOT NULL,
                {new_group_text} TEXT NOT NULL
                PRIMARY KEY {key});''')
            conn.commit()
