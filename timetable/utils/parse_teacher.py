import logging
import re

import pandas as pd

_logger = logging.getLogger(__name__)


def parse_teacher(lessons):
    """
    Преобразует каждый элемент колонки 'teacher' в элементы вида ['Фамилия И. О.', ...].
    Дополнительно возвращает список всех уникальных преподавателей.
    """
    _logger.info("Начинаю парсить 'teacher'...")

    teachers = []
    unique_teachers = set()
    for index, row in lessons.iterrows():
        teacher = row["teacher"]

        if pd.notna(teacher):
            result = re.findall(r"[А-Яа-яёЁ]+ +[А-Яа-яёЁ]\. +[А-Яа-яёЁ]\.", teacher)
            for i, item in enumerate(result):
                item = re.match(r"([А-Яа-яёЁ]+) +([А-Яа-яёЁ]\.) +([А-Яа-яёЁ]\.)", item)
                result[i] = item[1] + " " + item[2] + " " + item[3]

            teacher = result
            unique_teachers.update(set(teacher))

        teachers.append(teacher)
    lessons["teacher"] = teachers
    return lessons, list(unique_teachers)
