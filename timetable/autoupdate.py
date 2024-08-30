import logging
from profcomff_parse_lib import (parse_timetable, parse_all, parse_name,
                                 multiple_lessons, flatten, all_to_array,
                                 completion, to_id, check_date, delete_lesson,
                                 calc_date, post_event)
import pandas as pd
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql
import datetime

import pandas as pd
import sqlalchemy as sa
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from profcomff_parse_lib import (all_to_array, calc_date, check_date,
                                 completion, delete_lesson, dict_substitutions,
                                 flatten, multiple_lessons, parse_all,
                                 parse_name, parse_timetable, post_event,
                                 to_id)
from sqlalchemy.dialects import postgresql

DB_URI = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)
token = Variable.get("TOKEN_ROBOT_TIMETABLE")
headers = {"Authorization": f"{token}"}
environment = Variable.get("_ENVIRONMENT")


@task(
    task_id="parsing",
    inlets=Dataset("STG_RASPHYSMSU.raw_html"),
    outlets=Dataset("STG_RASPHYSMSU.old"),
)
def parsing():
    engine = sa.create_engine(DB_URI)
    timetables = pd.read_sql_query('select * from "STG_RASPHYSMSU".raw_html', engine)
    logging.info(
        f"timetables, columns: {len(list(timetables))} len: {timetables.shape[0]}"
    )
    results = pd.DataFrame()
    for i, row in timetables.iterrows():
        results = pd.concat([results, parse_timetable(row["raw_html"])])

    logging.info(f"results, columns: {len(list(results))} len: {results.shape[0]}")
    # parse_name, parse_all - Парсинг
    lessons = parse_name(results)
    logging.info(f"lessons, columns: {len(list(lessons))} len: {lessons.shape[0]}")
    lessons, places, groups, teachers, subjects = parse_all(lessons)
    # multiple lessons - Пары с одинаковыми временем, преподавателем и названием соединяет в одну
    # (аудитории могут быть разные, но теперь они лежат в одной строке.). Это в основном для английского.
    lessons = multiple_lessons(lessons)
    # flatten - Превращает в таблице place, teacher в массивы
    lessons = flatten(lessons)
    # all_to_array - Если совпадают время, название, преподаватели и команты (но не группы) то соединяет в одну строчку.
    # После этого действия и группы тоже становятся массивом.
    lessons = all_to_array(lessons)
    # completion - Если спарсились новые группы, преподаватели, комнаты, то добавляет их в бд.
    completion(groups, places, teachers, headers, environment)
    # to_id - превращает группы, преподов и комнаты в id в таблице
    lessons = to_id(lessons, headers, environment)
    engine.execute(
        """
       CREATE TABLE IF NOT EXISTS "STG_RASPHYSMSU"."new"(
           Id SERIAL PRIMARY key, subject varchar NOT NULL,
           odd bool NOT NULL, even bool NOT NULL,
           weekday INTEGER, num INTEGER,
           "start" varchar NOT NULL, "end" varchar NOT NULL,
           place INTEGER[], "group" INTEGER[],
           teacher INTEGER[], events_id INTEGER[] DEFAULT ARRAY[]::integer[]
       );
       CREATE TABLE IF NOT EXISTS "STG_RASPHYSMSU"."old"(
           Id SERIAL PRIMARY key, subject varchar NOT NULL,
           odd bool NOT NULL, even bool NOT NULL,
           weekday INTEGER, num INTEGER,
           "start" varchar NOT NULL, "end" varchar NOT NULL,
           place INTEGER[], "group" INTEGER[],
           teacher INTEGER[], events_id INTEGER[] DEFAULT ARRAY[]::integer[]
       );
       CREATE TABLE IF NOT EXISTS "STG_RASPHYSMSU".diff (
           subject varchar NULL, odd bool NULL,
           even bool NULL, weekday int4 NULL,
           num int4 NULL, "start" varchar NULL,
           "end" varchar NULL, place _int4 NULL,
           "group" _int4 NULL, teacher _int4 NULL,
           events_id _int4 NULL, id int4 NULL,
           "action" text NULL
    );
    """
    )
    engine.execute(
        """
        delete from "STG_RASPHYSMSU"."old";
        insert into "STG_RASPHYSMSU"."old" ("subject", "odd", "even", "weekday", "num", "start", "end", "place", "group",
        "teacher", "events_id")
        select "subject", "odd", "even", "weekday", "num", "start", "end", "place", "group", "teacher", "events_id"
        from "STG_RASPHYSMSU"."new";
        delete from "STG_RASPHYSMSU"."new";
        delete from "STG_RASPHYSMSU".diff;
    """
    )
    lessons.to_sql(
        name="new",
        con=engine,
        schema="STG_RASPHYSMSU",
        if_exists="append",
        index=False,
        dtype={
            "group": postgresql.ARRAY(sa.types.Integer),
            "teacher": postgresql.ARRAY(sa.types.Integer),
            "place": postgresql.ARRAY(sa.types.Integer),
            "subject": postgresql.VARCHAR,
            "odd": postgresql.BOOLEAN,
            "even": postgresql.BOOLEAN,
            "weekday": postgresql.INTEGER,
            "num": postgresql.INTEGER,
            "start": postgresql.VARCHAR,
            "end": postgresql.VARCHAR,
        },
    )
    logging.info("Задача 'parsing' выполнена.")


@task(
    task_id="find_diff",
    inlets=[Dataset("STG_RASPHYSMSU.old"), Dataset("STG_RASPHYSMSU.new")],
    outlets=Dataset("STG_RASPHYSMSU.diff"),
)
def find_diff():
    engine = sa.create_engine(DB_URI)
    logging.info("Начало задачи 'find_diff'")
    sql_query = """
    insert into "STG_RASPHYSMSU".diff ("subject", "odd", "even", "weekday", "num", "start", "end", "place", "group",
    "teacher", "events_id", "id", "action")
    select "subject", "odd", "even", "weekday", "num", "start", "end", "place", "group",
    "teacher", "events_id", "id", "action" from
    (select
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
        l.events_id as events_id ,
        r.id as id,
        CASE
            WHEN l.subject = r.subject THEN 'remember'
            WHEN l.subject IS NULL THEN 'create'
            WHEN r.subject IS NULL THEN 'delete'
    END AS action
    from "STG_RASPHYSMSU"."old" l
    full outer join "STG_RASPHYSMSU"."new" r
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
    order by l.subject) as tab;
    """
    engine.execute(sql_query)
    logging.info("Задача 'find_diff' выполнена.")


@task(task_id="update", outlets=Dataset("STG_RASPHYSMSU.new"))
def update():
    logging.info("Начало задачи 'update'")
    engine = sa.create_engine(DB_URI)
    lessons_for_deleting = pd.read_sql_query(
        """select events_id from "STG_RASPHYSMSU".diff
    where action='delete'""",
        engine,
    )
    lessons_for_creating = pd.read_sql_query(
        """select id, subject, "start", "end", "group",
    teacher, place, odd, even,
    weekday, num from "STG_RASPHYSMSU".diff where action='create'""",
        engine,
    )
    lessons_in_new = pd.read_sql_query(
        """select 
            id, subject, "start", "end", "group",
            teacher, place, odd, even,
            weekday, num 
        from "STG_RASPHYSMSU".new
        """,
        engine,
    )
    logging.info(f"Количество пар для удаления: {lessons_for_deleting.shape[0]}")
    logging.info(f"Количество пар для создания: {lessons_for_creating.shape[0]}")

    begin = datetime.datetime.now()
    begin = begin.strftime("%m/%d/%Y")
    end = Variable.get("SEMESTER_END")
    semester_start = Variable.get("SEMESTER_START")
    logging.info(
        f"Начало семестра: {semester_start}, дата начала загрузки пар: {begin}, дата конца семестра: {end}."
    )
    for i, row in lessons_for_deleting.iterrows():
        for id in row["events_id"]:
            if check_date(id, environment, begin):
                delete_lesson(headers, id, environment)
    lessons_new = calc_date(lessons_for_creating, begin, end, semester_start)
    logging.info(
        f"Из {lessons_for_creating.shape[0]} пар получилось {lessons_new.shape[0]} событий."
    )
    for i, row in lessons_new.iterrows():
        new_id = row["id"]
        #event_id = post_event(headers, row, environment)
        query = f'UPDATE "STG_RASPHYSMSU"."new" set events_id = events_id || array[{event_id}] WHERE id={new_id}'
        engine.execute(query)
    query = """
    UPDATE "STG_RASPHYSMSU"."new" as ch
    SET events_id = ch.events_id || selected.events_id
    FROM
    (SELECT id, events_id, "action" from "STG_RASPHYSMSU".diff) AS Selected
    WHERE ch.id  = Selected.id and selected."action" = 'remember';
    """
    engine.execute(query)
    logging.info("Задача 'update' выполнена.")
    lessons_in_new_w_dates = calc_date(lessons_in_new, begin, end, semester_start)
    query = """
    delete from "STG_RASPHYSMSU"."new_with_dates";
    """
    engine.execute(query)
    lessons_in_new_w_dates.to_sql(
        name="link_new_with_dates",
        con=engine,
        schema="STG_RASPHYSMSU",
        if_exists="append",
        index=False,
        dtype={
            "group": postgresql.ARRAY(sa.types.Integer),
            "teacher": postgresql.ARRAY(sa.types.Integer),
            "place": postgresql.ARRAY(sa.types.Integer),
            "subject": postgresql.VARCHAR,
            "odd": postgresql.BOOLEAN,
            "even": postgresql.BOOLEAN,
            "weekday": postgresql.INTEGER,
            "num": postgresql.INTEGER,
            "start": postgresql.VARCHAR,
            "end": postgresql.VARCHAR,
        },
    )
    query = """
    insert into "STG_RASPHYSMSU"."new_with_dates" ("subject", "odd", "even", "weekday", "num", "start", "end", "place", "group",
        "teacher", "events_id")
        select
            cast(FLOOR(RANDOM() * 10000000000) as int) as id,
            coalesce(link."subject", new."subject"),
            new."odd" as "odd",
            new."even" as "even",
            new."weekday" as "weekday",
            new."num" as "num",
            link."start" as "start",
            link."end" as "end",
            new."place" as "place",
            new."group" as "group",
            new."teacher" as "teacher",
            new."events_id" as "events_id"
        from "STG_RASPHYSMSU"."link_new_with_dates" as link
        left join "STG_RASPHYSMSU"."new" as new
        on link.id = new.id;
    """
    engine.execute(query)
    logging.info("Из new добавлены события на дату в new_with_dates")


@dag(
    schedule=[Dataset("STG_RASPHYSMSU.raw_html")],
    start_date=datetime.datetime(2023, 8, 1, 2, 0, 0),
    max_active_runs=1,
    catchup=False,
    tags=["dwh", "timetable"],
    default_args={
        "owner": "zamyatinsv",
        "retries": 0,
        "retry_delay": datetime.timedelta(minutes=5),
    },
)
def update_timetable():
    parsing() >> find_diff() >> update()


timetable_sync = update_timetable()
