"""
Этот модуль содержит классы для парсинга сайта расписания и
запускающую функцию.
"""

import logging
import uuid
from typing import Any, List, Dict
from datetime import datetime, timedelta
import pandas as pd
import sqlalchemy as sa
import requests
from bs4 import BeautifulSoup
from airflow import DAG
from airflow.datasets import Dataset
from airflow.decorators import dag, task
from airflow.models import Connection, Variable
from airflow.exceptions import AirflowException

_logger = logging.getLogger(__name__)

# [[курс, поток, количество групп], ...]
SOURCES = [
    [1, 1, 9],
    [1, 2, 9],
    [2, 1, 9],
    [2, 2, 9],
    [3, 1, 10],
    [3, 2, 8],
    [4, 1, 10],
    [4, 2, 8],
    [5, 1, 13],
    [5, 2, 12],
    [6, 1, 13],
    [6, 2, 11],
]


def instead_request(course, stream, group, conn):
    cursor = conn.cursor()
    postgreSQL_select_Query = 'SELECT * FROM "STG_TIMETABLE".raw_html_old'
    cursor.execute(postgreSQL_select_Query)
    pages = cursor.fetchall()
    url = f"http://ras.phys.msu.ru/table/{course}/{stream}/{group}.htm"
    for page in pages:
        if page[0] == url:
            return page[1]


class Group:
    def __init__(self, html: BeautifulSoup):
        self.data = html
        self.tags: List[BeautifulSoup] = []
        self.result: List[Dict[str, Any]] = []

    def run(self) -> List[Dict[str, Any]]:
        body = self.data.select("body")[0]
        try:
            rows = body.select("tr")[1]
        except IndexError:
            return []

        self.tags = list(filter(lambda x: x != "\n", rows.children))

        group = self.get_group()
        lessons = self.get_lessons()
        for lesson in lessons:
            self.result.append(dict(**lesson, group=group))
        return self.result

    def get_group(self) -> str:
        """Номер группы из заголовка страницы."""
        try:
            return self.tags[1].text
        except IndexError:
            return ""

    def get_lessons(self) -> List[Dict[str, Any]]:
        """Список предметов."""
        lessons: List[Dict[str, Any]] = []

        # Разделяем self.tag по дням.
        tags_day: List[List[BeautifulSoup]] = []
        temp_day: List[BeautifulSoup] = []
        for tag in self.tags[3:-1]:
            # Новый день.
            if len(tag.select("td.delimiter")) != 0:
                tags_day.append(temp_day)
                temp_day = []
                continue
            temp_day.append(tag)

        # Разделяем self.tag по номеру пары.
        tags_num: List[List[List[BeautifulSoup]]] = []
        temp_day: List[List[BeautifulSoup]] = []
        temp_num: List[BeautifulSoup] = []
        for tags in tags_day:
            for tag in tags:
                if len(tag.select("td.tdtime")) != 0:
                    if temp_num:
                        # noinspection PyTypeChecker
                        # Здесь PyCharm ругается, что тип не List[BeautifulSoup], что справедливо.
                        temp_day.append(temp_num)
                        temp_num = []
                temp_num.append(tag)
            tags_num.append(temp_day)
            temp_day = []
            temp_num = []

        num2start_end = {
            0: ("9:00", "10:35"),
            1: ("10:50", "12:25"),
            2: ("13:30", "15:05"),
            3: ("15:20", "16:55"),
            4: ("17:05", "18:40"),
            5: ("18:55", "20:30"),
        }
        for weekday, tags_day in enumerate(tags_num):
            for lesson_num, tags_lessons in enumerate(tags_day):
                for tag in tags_lessons:
                    results = Lesson(tag).run()
                    for lesson in results:
                        lesson["weekday"] = weekday
                        lesson["num"] = lesson_num

                        lesson["start"] = num2start_end[lesson_num][0]
                        lesson["end"] = num2start_end[lesson_num][1]

                        lesson["name"] = lesson["name"].replace("\xa0", " ")
                        if lesson["name"] != " ":
                            lessons.append(lesson)
        return lessons


class Lesson:
    def __init__(self, html: BeautifulSoup):
        self.html = html
        self.result: Dict[str, Any] = {}
        self.type = self.define_type()

    # Возвращает List, поскольку возможны ситуации, когда в одном tag две пары.
    def run(self) -> List[Dict[str, Any]]:
        if self.type == "KIND_ITEM1":
            self.result = self._get_item1()
        elif self.type == "KIND_SMALL1_WITH_TIME":
            self.result = self._get_small1_with_time()
        elif self.type == "KIND_SMALL1_WITHOUT_TIME":
            self.result = self._get_small1_without_time()
        elif self.type == "KIND_ITEM1_WITH_SMALL0":
            self.result = self._get_item1_with_small0()
        elif self.type == "KIND_SMALL1_WITH_SMALL0_WITH_TIME":
            self.result = self._get_small1_with_small0_with_time()
        elif self.type == "KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME":
            self.result = self._get_small1_with_small0_without_time()
        else:
            raise RuntimeError("Unexpected type of lesson")
        return self.result

    def define_type(self):
        if len(self.html.select("td.tditem1")) != 0:
            if len(self.html.select("td.tdsmall0")) != 0:
                return "KIND_ITEM1_WITH_SMALL0"
            return "KIND_ITEM1"

        if len(self.html.select("td.tdsmall1")) != 0:
            if len(self.html.select("td.tdsmall0")) != 0:
                if len(self.html.select("td.tdtime")) != 0:
                    return "KIND_SMALL1_WITH_SMALL0_WITH_TIME"
                else:
                    return "KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME"
            else:
                if len(self.html.select("td.tdtime")) != 0:
                    return "KIND_SMALL1_WITH_TIME"
                else:
                    return "KIND_SMALL1_WITHOUT_TIME"

    def _get_item1(self):
        html = self.html.select("td.tditem1")[0]
        return [
            {
                "name": "".join(str(tag) for tag in html.contents),
                "odd": True,
                "even": True,
            }
        ]

    def _get_small1_with_time(self):
        html = self.html.select("td.tdsmall1")[0]
        return [
            {
                "name": "".join(str(tag) for tag in html.contents),
                "odd": True,
                "even": False,
            }
        ]

    def _get_small1_without_time(self):
        html = self.html.select("td.tdsmall1")[0]
        return [
            {
                "name": "".join(str(tag) for tag in html.contents),
                "odd": False,
                "even": True,
            }
        ]

    def _get_item1_with_small0(self):
        tags = self.html.select("td.tdsmall0")

        results = []
        for index, html in enumerate(tags):
            results.append(
                {
                    "name": "".join(str(tag) for tag in html.contents),
                    "odd": True,
                    "even": True,
                }
            )
        return results

    def _get_small1_with_small0_with_time(self):
        tags = self.html.select("td.tdsmall0")

        results = []
        for index, html in enumerate(tags):
            results.append(
                {
                    "name": "".join(str(tag) for tag in html.contents),
                    "odd": True,
                    "even": False,
                }
            )
        return results

    def _get_small1_with_small0_without_time(self):
        tags = self.html.select("td.tdsmall0")

        results = []
        for index, html in enumerate(tags):
            results.append(
                {
                    "name": "".join(str(tag) for tag in html.contents),
                    "odd": False,
                    "even": True,
                }
            )
        return results


def run(html: str) -> List[Dict[str, Any]]:
    html = BeautifulSoup(html, "html.parser")
    return Group(html).run()


USER_AGENT = (
    "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
)
HEADERS = {"User-Agent": USER_AGENT}


def parse_timetable_for_group(html) -> pd.DataFrame:
    """
    :param html: HTML страницы расписания.
    :return: DataFrame с результатами парсинга.
    """
    results = pd.DataFrame(run(html))
    return results


def parse_timetable():
    result = pd.DataFrame()
    for source in SOURCES:
        for group in range(1, source[2] + 1):
            result = pd.concat(
                [
                    result,
                    parse_timetable_for_group(
                        f"http://ras.phys.msu.ru/table/{source[0]}/{source[1]}/{group}.htm"
                    ),
                ]
            )
    return result


DB_DSN = (
    Connection.get_connection_from_secrets("postgres_dwh")
    .get_uri()
    .replace("postgres://", "postgresql://")
    .replace("?__extra__=%7B%7D", "")
)


@task(
    task_id="raw_timetable_parse",
    inlets=Dataset("STG_RASPHYSMSU.raw_html"),
    outlets=Dataset("ODS_TIMETABLE.ods_timetable_act"),
)
def raw_timetable_parse():
    sql_engine = sa.create_engine(DB_DSN)
    conn = sql_engine.connect()
    result = []
    for source in SOURCES:
        for group in range(1, source[2] + 1):
            url = f"http://ras.phys.msu.ru/table/{source[0]}/{source[1]}/{group}.htm"
            group_html = conn.execute(
                f"""
                    DELETE FROM "ODS_TIMETABLE".ods_timetable_act;
                    SELECT * FROM "STG_RASPHYSMSU".raw_html WHERE url = '{url}'
                """
            ).one_or_none()
            if not group_html:
                logging.info(f"{url} is not in STG_RASPHYSMSU.raw_html")
                raise AirflowException(f"Failed to get html for {source}")
            group_result = parse_timetable_for_group(group_html.raw_html)
            result.append(group_result)
    result = pd.concat(result, ignore_index=True)
    logging.info(result.info(verbose=True))
    result['id'] = [uuid.uuid4() for _ in range(len(result.index))]
    result.to_sql(
        "ods_timetable_act",
        schema="ODS_TIMETABLE",
        con=sql_engine,
        if_exists="append",
        index=False,
    )
    return Dataset("ODS_TIMETABLE.ods_timetable_act")


with DAG(
    dag_id="ODS_TIMETABLE.ods_timetable_act",
    schedule="@daily",
    start_date=datetime(2024, 11, 24),
    tags=["ods", "rasphysmsu"],
    default_args={
        "owner": "zimovchik",
        "retries": 1,
    },
) as dag:
    raw_timetable_parse()
