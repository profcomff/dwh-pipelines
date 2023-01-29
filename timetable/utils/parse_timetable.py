"""
Этот модуль содержит классы для парсинга сайта расписания и
запускающую функцию.
"""
import logging
from typing import Any, List, Dict

import pandas as pd
import requests
from bs4 import BeautifulSoup

_logger = logging.getLogger(__name__)


class Group:
    def __init__(self, html: BeautifulSoup):
        self.data = html
        self.tags: List[BeautifulSoup] = []
        self.result: List[Dict[str, Any]] = []

    def run(self) -> List[Dict[str, Any]]:
        body = self.data.select("body")[0]
        rows = body.select("tr")[1]
        self.tags = list(filter(lambda x: x != "\n", rows.children))

        group = self.get_group()
        lessons = self.get_lessons()
        for lesson in lessons:
            self.result.append(dict(**lesson, group=group))
        return self.result

    def get_group(self) -> str:
        """Номер группы из заголовка страницы."""
        return self.tags[1].text

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

        num2start_end = {0: ("9:00", "10:35"), 1: ("10:50", "12:25"), 2: ("13:30", "15:05"),
                         3: ("15:20", "16:55"), 4: ("17:05", "18:40"), 5: ("18:55", "20:30")}
        for weekday, tags_day in enumerate(tags_num):
            for lesson_num, tags_lessons in enumerate(tags_day):
                for tag in tags_lessons:
                    results = Lesson(tag).run()
                    for lesson in results:
                        lesson["weekday"] = weekday
                        lesson["num"] = lesson_num

                        lesson["start"] = num2start_end[lesson_num][0]
                        lesson["end"] = num2start_end[lesson_num][1]

                        lesson['name'] = lesson['name'].replace("\xa0", " ")
                        if lesson['name'] != " ":
                            lessons.append(lesson)
        return lessons


class Lesson:
    def __init__(self, html: BeautifulSoup):
        self.html = html
        self.result: Dict[str, Any] = {}
        self.type = self.define_type()

    # Возвращает List, поскольку возможны ситуации, когда в одном tag две пары.
    def run(self) -> List[Dict[str, Any]]:
        if self.type == 'KIND_ITEM1':
            self.result = self._get_item1()
        elif self.type == 'KIND_SMALL1_WITH_TIME':
            self.result = self._get_small1_with_time()
        elif self.type == 'KIND_SMALL1_WITHOUT_TIME':
            self.result = self._get_small1_without_time()
        elif self.type == 'KIND_ITEM1_WITH_SMALL0':
            self.result = self._get_item1_with_small0()
        elif self.type == 'KIND_SMALL1_WITH_SMALL0_WITH_TIME':
            self.result = self._get_small1_with_small0_with_time()
        elif self.type == 'KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME':
            self.result = self._get_small1_with_small0_without_time()
        else:
            raise RuntimeError('Unexpected type of lesson')
        return self.result

    def define_type(self):
        if len(self.html.select("td.tditem1")) != 0:
            if len(self.html.select("td.tdsmall0")) != 0:
                return 'KIND_ITEM1_WITH_SMALL0'
            return 'KIND_ITEM1'

        if len(self.html.select("td.tdsmall1")) != 0:
            if len(self.html.select("td.tdsmall0")) != 0:
                if len(self.html.select("td.tdtime")) != 0:
                    return 'KIND_SMALL1_WITH_SMALL0_WITH_TIME'
                else:
                    return 'KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME'
            else:
                if len(self.html.select("td.tdtime")) != 0:
                    return 'KIND_SMALL1_WITH_TIME'
                else:
                    return 'KIND_SMALL1_WITHOUT_TIME'

    def _get_item1(self):
        html = self.html.select("td.tditem1")[0]
        return [{"name": "".join(str(tag) for tag in html.contents), "odd": True, "even": True}]

    def _get_small1_with_time(self):
        html = self.html.select("td.tdsmall1")[0]
        return [{"name": "".join(str(tag) for tag in html.contents), "odd": True, "even": False}]

    def _get_small1_without_time(self):
        html = self.html.select("td.tdsmall1")[0]
        return [{"name": "".join(str(tag) for tag in html.contents), "odd": False, "even": True}]

    def _get_item1_with_small0(self):
        tags = self.html.select("td.tdsmall0")

        results = []
        for index, html in enumerate(tags):
            results.append({"name": "".join(str(tag) for tag in html.contents), "odd": True, "even": True})
        return results

    def _get_small1_with_small0_with_time(self):
        tags = self.html.select("td.tdsmall0")

        results = []
        for index, html in enumerate(tags):
            results.append({"name": "".join(str(tag) for tag in html.contents), "odd": True, "even": False})
        return results

    def _get_small1_with_small0_without_time(self):
        tags = self.html.select("td.tdsmall0")

        results = []
        for index, html in enumerate(tags):
            results.append({"name": "".join(str(tag) for tag in html.contents), "odd": False, "even": True})
        return results


def run(html: str) -> List[Dict[str, Any]]:
    html = BeautifulSoup(html, "html.parser")
    return Group(html).run()


def parse_timetable():
    """
    Получает данные с сайта расписания.
    """
    # [[курс, поток, количество групп], ...]
    sources = [
        [1, 1, 6], [1, 2, 6], [1, 3, 6],
        [2, 1, 6], [2, 2, 6], [2, 3, 6],
        [3, 1, 10], [3, 2, 8],
        [4, 1, 10], [4, 2, 8],
        [5, 1, 13], [5, 2, 12],
        [6, 1, 13], [6, 2, 11]
    ]

    USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 " \
                 "(KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
    HEADERS = {"User-Agent": USER_AGENT}

    _logger.info("Начинаю парсинг сайта расписания...")
    results = pd.DataFrame()
    for index, source in enumerate(sources):
        for group in range(1, source[2] + 1):
            try:
                html = requests.get(f"http://ras.phys.msu.ru/table/{source[0]}/{source[1]}/{group}.htm",
                                    headers=HEADERS).text
                results = pd.concat([results, pd.DataFrame(run(html))])
            except Exception:
                _logger.warning(f"'{source[0]}/{source[1]}/{group}' парсинг завершился ошибкой.")
                raise

    return results
