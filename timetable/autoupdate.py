import logging
from profcomff_parse_lib import (parse_timetable, parse_all, parse_name,
                                 multiple_lessons, flatten, all_to_array,
                                 completion, to_id, check_date, delete_lesson,
                                 calc_date, post_event)
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import datetime

from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable


DB_URI = Connection.get_connection_from_secrets('postgres_dwh').get_uri().replace("postgres://", "postgresql://")
token = Variable.get("TOKEN_ROBOT_TIMETABLE_TEST")
headers = {"Authorization": f"{token}"}
engine = sa.create_engine(DB_URI)
conn = engine.connect()


@task(task_id='parsing', outlets=Dataset("STG_TIMETABLE.raw_html"))
def parsing(base):
    timetables = pd.read_sql_query('select * from "STG_TIMETABLE".raw_html', engine)
    results = pd.DataFrame()
    for i, row in timetables.iterrows():
        results = pd.concat([results, parse_timetable(row["raw_html"])])

    # parse_name, parse_all - Парсинг
    lessons = parse_name(results)
    lessons, places, groups, teachers, subjects = parse_all(lessons)
    # multiple lessons - Пары с одинаковыми временем, преподавателем и названием соединяет в одну
    # (аудитории могут быть разные, но теперь они лежат в одной строке.). Это в основном для английского.
    lessons = multiple_lessons(lessons)
    # flatten - Првращает в таблице place, teacher в массивы
    lessons = flatten(lessons)
    # all_to_array - Если совпадают время, название, преподаватели и команты (но не группы) то соединяет в одну строчку.
    # После этого действия и группы тоже становятся массивом.
    lessons = all_to_array(lessons)
    # comletion - Если спарсились новые группы, преподаватели, комнаты, то добавляет их в бд.
    completion(groups, places, teachers, headers, base)
    # to_id - превращает группы, преподов и комнаты в id в таблице
    lessons = to_id(lessons, headers, base)
    conn.execute("""
       CREATE TABLE IF NOT EXISTS "STG_TIMETABLE"."new"(
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
       DROP TABLE IF EXISTS "STG_TIMETABLE"."old";
       DROP TABLE IF EXISTS "STG_TIMETABLE"."diff";
       ALTER TABLE IF EXISTS "STG_TIMETABLE"."new" RENAME TO "old";
       """)
    lessons.to_sql(name="new", con=engine, schema="STG_TIMETABLE", if_exists="replace", index=False,
                   dtype={"group": postgresql.ARRAY(sa.types.Integer), "teacher": postgresql.ARRAY(sa.types.Integer),
                          "place": postgresql.ARRAY(sa.types.Integer)})


@task(task_id='find_diff')
def find_diff():
    logging.info("Начало задачи diff")
    conn.execute("""
    ALTER table "STG_TIMETABLE"."new" ADD id SERIAL PRIMARY key;
    ALTER table "STG_TIMETABLE"."new" ADD events_id INTEGER[];
    UPDATE "STG_TIMETABLE"."new" set events_id =  ARRAY[]::integer[];
    """)
    sql_query = """
    create table "STG_TIMETABLE".diff as
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
    from "STG_TIMETABLE"."old" l
    full outer join "STG_TIMETABLE"."new" r
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
def update(base):
    logging.info("Начало задачи update")
    lessons_for_deleting = pd.read_sql_query("""select events_id from "STG_TIMETABLE".diff where action='delete'""", engine)
    lessons_for_creating = pd.read_sql_query("""select id, subject, "start", "end", "group", teacher, place, odd, even,
    weekday, num from "STG_TIMETABLE".diff where action='create'""", engine)

    begin = datetime.datetime.now()
    begin = begin.strftime("%m/%d/%Y")
    end = Variable.get("SEMESTER_END_TEST")
    semester_start = Variable.get("SEMESTER_START_TEST")
    for i, row in lessons_for_deleting.iterrows():
        for id in row["events_id"]:
            if check_date(id, base, begin):
                delete_lesson(headers, id, base)
    lessons_new = calc_date(lessons_for_creating, begin, end, semester_start)
    for i, row in lessons_new.iterrows():
        new_id = row["id"]
        event_id = post_event(headers, row, base)
        query = f'UPDATE "STG_TIMETABLE"."new" set events_id = events_id || array[{event_id}] WHERE id={new_id}'
        conn.execute(query)
    query = """
    UPDATE "STG_TIMETABLE"."new" as ch
    SET events_id = ch.events_id || selected.events_id
    FROM
    (SELECT id, events_id, "action" from "STG_TIMETABLE".diff) AS Selected
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
def update_timetable(base):
    parsing(base) >> find_diff() >> update(base)


timetable_sync = update_timetable("test")
