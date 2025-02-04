import logging
from profcomff_parse_lib import (parse_timetable, parse_all, parse_name,
                                 multiple_lessons, flatten, all_to_array,
                                 completion, check_date, delete_lesson,
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
                                 parse_name, parse_timetable, post_event)
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

import logging
import sys

import requests

from profcomff_parse_lib.utilities import urls_api

_logger = logging.getLogger(__name__)

import enum

# MODES = enum.Enum("Modes", "test prod")
# mode = MODES.test


MODES_URL = enum.Enum("Modes", "get delete post patch")


def get_url_room(mode_, base):
    if mode_ == MODES_URL.get:
        return get_url(base) + "/timetable/room/?limit=0&offset=0"
    if mode_ == MODES_URL.delete:
        return get_url(base) + '/timetable/room/'
    if mode_ == MODES_URL.post:
        return get_url(base) + '/timetable/room/'
    if mode_ == MODES_URL.patch:
        return get_url(base) + '/timetable/room/'


def get_url_group(mode_, base):
    if mode_ == MODES_URL.get:
        return get_url(base) + "/timetable/group/?limit=0&offset=0"
    if mode_ == MODES_URL.delete:
        return get_url(base) + '/timetable/group/'
    if mode_ == MODES_URL.post:
        return get_url(base) + '/timetable/group/'
    if mode_ == MODES_URL.patch:
        return get_url(base) + '/timetable/group/'


def get_url_lecturer(mode_, base):
    if mode_ == MODES_URL.get:
        return get_url(base) + "/timetable/lecturer/?limit=0&offset=0"
    if mode_ == MODES_URL.delete:
        return get_url(base) + '/timetable/lecturer/'
    if mode_ == MODES_URL.post:
        return get_url(base) + '/timetable/lecturer/'
    if mode_ == MODES_URL.patch:
        return get_url(base) + '/timetable/lecturer/'


def get_url_event(mode_, base):
    if mode_ == MODES_URL.get:
        return get_url(base) + "/timetable/event/"
    if mode_ == MODES_URL.delete:
        return get_url(base) + '/timetable/event/'
    if mode_ == MODES_URL.post:
        return get_url(base) + '/timetable/event/'
    if mode_ == MODES_URL.patch:
        return get_url(base) + '/timetable/event/'


TEST_URL = "https://api.test.profcomff.com"
PROD_URL = "https://api.profcomff.com"

def get_url(base):
    if base == "prod":
        return PROD_URL
    if base == "test":
        return TEST_URL

def room_to_id(lessons, headers, base):
    """
    Превращает названия комнат в расписании в id для базы данных.
    """
    _logger.info("Превращаю названия комнат в id...")

    response = requests.get(urls_api.get_url_room(urls_api.MODES_URL.get, base), headers=headers)
    rooms = response.json()["items"]

    place = lessons["place"].tolist()
    for i, row in lessons.iterrows():
        for k, ob in enumerate(row["place"]):
            b = False
            for room in rooms:
                if ob == room["name"]:
                    place[i][k] = room["id"]
                    b = True
                    break
            if not b:
                # @mixx3 мы согласны на потери данных в таком случае
                # _logger.critical("Ошибка, аудитория '{aud}' не найдена. Завершение работы".format(aud=row['place']))
                # sys.exit()
                _logger.info("Ошибка, аудитория '{aud}' не найдена. ".format(aud=row['place']))
                place[i][k] = -100  # чтобы не было ошибок в связях
    lessons["place"] = place
    return lessons


def group_to_id(lessons, headers, base):
    """
    Превращает названия групп в расписании в id для базы данных.
    """
    _logger.info("Превращаю названия групп в id...")

    response = requests.get(urls_api.get_url_group(urls_api.MODES_URL.get, base), headers=headers)
    groups = response.json()["items"]

    new_groups = lessons["group"].tolist()
    for i, row in lessons.iterrows():
        for j in range(len(row["group"])):
            b = False
            for group in groups:
                if row["group"][j] == group["number"]:
                    new_groups[i][j] = group["id"]
                    b = True
                    break
            if not b:
                body = {"name": f"Группа # {row['group'][j]}", 'number': row['group'][j]}
                response = requests.request(urls_api.get_url_group(urls_api.MODES_URL.post, base), headers=headers, json=body)
                new_groups[i][j] = response.json()["id"]
                _logger.info(f'Новая группа: {response}')
    lessons["group"] = new_groups

    return lessons


def teacher_to_id(lessons, headers, base):
    """
    Превращает препов в расписании в id для базы данных.
    """
    _logger.info("Превращаю преподавателей в id...")

    response = requests.get(urls_api.get_url_lecturer(urls_api.MODES_URL.get, base), headers=headers)
    teachers = response.json()["items"]

    new_teacher = lessons["teacher"].tolist()
    for i, row in lessons.iterrows():
        for j, item_ in enumerate(row["teacher"]):
            b = False
            for teacher in teachers:
                item = item_.split()
                b1 = item[0] == teacher['last_name']
                b2 = item[1][0] == teacher['first_name'][0]
                b3 = item[2][0] == teacher['middle_name'][0]
                b = b1 and b2 and b3
                if b:
                    new_teacher[i][j] = teacher["id"]
                    break
            if not b:
                item = item_.split()
                body = {"first_name": item[1][0], "middle_name": item[2][0], "last_name": item[0], "description": "Преподаватель физического факультета" }
                response = requests.request(urls_api.get_url_lecturer(urls_api.MODES_URL.post, base), headers=headers,
                                            json=body)
                new_teacher[i][j] = response.json()["id"]
                _logger.info(f'Новый преподаватель: {response}')
    lessons["teacher"] = new_teacher

    return lessons


def to_id(lessons, headers, base):
    lessons = room_to_id(lessons, headers, base)
    lessons = group_to_id(lessons, headers, base)
    lessons = teacher_to_id(lessons, headers, base)
    return lessons


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
        event_id = []
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
    insert into "STG_RASPHYSMSU"."new_with_dates" ("id", "subject", "odd", "even", "weekday", "num", "start", "end", "place", "group",
        "teacher", "events_id")
        select 
            "id", "subject", "odd", "even", "weekday", "num", "start", "end", "place", "group", "teacher", "events_id"
        from(
            select 
            cast(FLOOR(RANDOM() * 100000) as int) as id,
            coalesce(link."subject", new."subject") as "subject",
            new."odd" as "odd",
            new."even" as "even",
            new."weekday" as "weekday",
            new."num" as "num",
            link."start" as "start",
            link."end" as "end",
            new."place" as "place",
            new."group" as "group",
            new."teacher" as "teacher",
            new."events_id" as "events_id",
            row_number() over (partition by link.subject, link."start", link.group, link.place) as "rn"
        from "STG_RASPHYSMSU"."new" as new
        left join "STG_RASPHYSMSU"."link_new_with_dates" as link
        on link.subject = new.subject and link.teacher = new.teacher and link.group = new.group and link.place = new.place
        )
        where "rn"=1;
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
