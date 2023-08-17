import logging
from profcomff_parse_lib import *
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


DB_URI = Connection.get_connection_from_secrets('data_base').get_uri().replace("postgres://", "postgresql://")
token = Variable.get("token_db")
headers = {"Authorization": f"{token}"}
engine = sa.create_engine(DB_URI)
conn = engine.connect()
conn.execute('ALTER USER postgres SET search_path TO "STG_TIMETABLE";')


@task(task_id='parsing', outlets=Dataset("STG_TIMETABLE.raw_html"))
def parsing():
    timetables = pd.read_sql_query('select * from raw_html', engine)
    results = pd.DataFrame()
    for i, row in timetables.iterrows():
        results = pd.concat([results, parse_timetable(row["raw_html"])])

    lessons = parse_name(results)
    lessons, places, groups, teachers, subjects = parse_all(lessons)
    lessons = manual_edit(lessons)
    lessons = multiple_lessons(lessons)
    lessons = flatten(lessons)
    lessons = all_to_array(lessons)
    completion(groups, places, teachers, headers, "test")
    lessons = to_id(lessons, headers, "test")
    conn.execute("""
       CREATE TABLE IF NOT EXISTS "new"(
           Id SERIAL PRIMARY key,
           subject varchar NOT NULL,
           odd bool NOT NULL,
           even bool NOT NULL,
           weekday INTEGER,
           num INTEGER,
           "start" varchar NOT NULL,
           "end" varchar NOT NULL,
           place INTEGER[],
           "group" INTEGER[],
           teacher INTEGER[],
           events_id INTEGER[]
       );
       DROP TABLE IF EXISTS "old";
       DROP TABLE IF EXISTS "diff";
       ALTER TABLE IF EXISTS "new" RENAME TO "old";
       """)
    lessons.to_sql(name="new", con=engine, if_exists="replace", index=False,
                   dtype={"group": postgresql.ARRAY(sa.types.Integer), "teacher": postgresql.ARRAY(sa.types.Integer),
                          "place": postgresql.ARRAY(sa.types.Integer)})


@task(task_id='find_diff')
def find_diff():
    logging.info("Начало задачи diff")
    conn.execute("""
    ALTER table "new" ADD id SERIAL PRIMARY key;
    ALTER table "new" ADD events_id INTEGER[];
    UPDATE "new" set events_id =  ARRAY[]::integer[];
    """)
    sql_query = """
    create table diff as
    select
        coalesce(l.subject, r.subject) as subject,
        coalesce(l.odd, r.odd) as odd,
        coalesce(l.even, r.even) as even,
        coalesce(l.weekday, r.weekday) as weekday,
        coalesce(l.num, r.num) as num,
        coalesce(l.start, r.start) as start,
        coalesce(l.end, r.end) as end,
        coalesce(l.place, r.place) as place,
        coalesce(l.group, r.group) as group,
        coalesce(l.teacher, r.teacher) as teacher,
        l.events_id,
        r.id,
        CASE
            WHEN l.subject = r.subject THEN 'remember'
            WHEN l.subject IS NULL THEN 'create'
            WHEN r.subject IS NULL THEN 'delete'
    END AS action
    from "old" l
    full outer join "new" r
        on l.subject = r.subject
        and  l.odd = r.odd
        and  l.even = r.even
        and  l.weekday  = r.weekday
        and  l.num  = r.num
        and  l.start = r.start
        and  l.end = r.end
        and  (l.place  <@ r.place  and l.place  @> r.place)
        and  (l.group  <@ r.group  and l.group  @> r.group)
        and  (l.teacher  <@ r.teacher  and l.teacher  @> r.teacher)
    order by l.subject;
    """
    conn.execute(sql_query)


@task(task_id='update')
def update():
    logging.info("Начало задачи update")
    lessons_for_deleting = pd.read_sql_query("""select events_id from diff where action='delete'""", engine)
    lessons_for_creating = pd.read_sql_query("""select id, subject, "start", "end", "group", teacher, place, odd, even,
    weekday, num from diff where action='create'""", engine)

    begin = datetime.datetime.now()
    end = datetime.datetime.now() + datetime.timedelta(days=3)
    begin = begin.strftime("%m/%d/%Y")
    end = end.strftime("%m/%d/%Y")
    for i, row in lessons_for_deleting.iterrows():
        for id in row["events_id"]:
            if check_date(id, "test", begin):
                delete_lesson(headers, id, "test")
    lessons_new = calc_date(lessons_for_creating, begin, end, "07/24/2023")
    for i, row in lessons_new.iterrows():
        new_id = row["id"]
        event_id = post_event(headers, row, "test")
        query = f'UPDATE "new" set events_id = events_id || array[{event_id}] WHERE id={new_id}'
        conn.execute(query)
    query = """
    UPDATE "new" as ch
    SET events_id = ch.events_id || selected.events_id
    FROM
    (SELECT id, events_id, "action" from diff) AS Selected
    WHERE ch.id  = Selected.id and selected."action" = 'remember';
    """
    conn.execute(query)


@dag(
    schedule='0 */6 * * *',
    start_date=datetime.datetime(2023, 8, 1, 2, 0, 0),
    catchup=False,
    tags= ["UPDATE"],
    default_args={
        "owner": "dwh",
        "retries": 0,
        "retry_delay": datetime.timedelta(minutes=5)
    }
)
def update_timetable():
    parsing() >> find_diff() >> update()


timetable_sync = update_timetable()
