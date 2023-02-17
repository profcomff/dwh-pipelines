import logging
import re

import pandas as pd

_logger = logging.getLogger(__name__)


def _parse_name(name):
    """
    Разделяет одно 'name' на 'subject', 'teacher' и 'place' по заданным регулярным выражениям.
    В случае отсутствия подходящего регулярного выражения ставит 'subject' равным 'name' и выдает предупреждение.
    В 'subject' включен номер группы, если он указан в названии.
    """
    parsed_name = {"subject": None, "teacher": None, "place": None}

    # '... <nobr>5-27</nobr> проф. Чиркин А. С.'
    result = re.match(r"([А-Яа-яёЁa-zA-Z +,/.\-\d]+)<nobr>([А-Яа-яёЁa-zA-Z +,/.\-\d]+)</nobr>" +
                      r"([А-Яа-яёЁa-zA-Z +,/.\-\d]+)", name)
    if not (result is None):
        if name == result[0]:
            parsed_name["subject"] = result[1]
            parsed_name["place"] = result[2]
            parsed_name["teacher"] = result[3]
            return parsed_name

    # '... <nobr>Каф.</nobr>'
    result = re.match(r'([А-Яа-яёЁa-zA-Z +,/.\-\d]+)<nobr>([А-Яа-яёЁa-zA-Z +,/.\-0\d]+)</nobr> *', name)
    if not (result is None):
        if name == result[0]:
            parsed_name["subject"] = result[1]
            parsed_name["place"] = result[2]
            return parsed_name

    # 'Специальный физический практикум (Андрианов Т. А.){i}'
    for i in range(2):
        result = re.match(r"([А-Яа-яёЁa-zA-Z +,/\-\d]+) ([А-Яа-яёЁa-zA-Z]+ [А-Яа-яёЁa-zA-Z]\. [А-Яа-яёЁa-zA-Z]\.)" +
                          r" ([А-Яа-яёЁa-zA-Z]+ [А-Яа-яёЁa-zA-Z]\. [А-Яа-яёЁa-zA-Z]\.)" * i + " *", name)
        if not (result is None):
            if name == result[0]:
                parsed_name["subject"] = result[1]
                parsed_name["teacher"] = "".join([result[j + 2] + " " for j in range(i + 1)])
                return parsed_name

    # 'Ядерный практикум'
    # Обратите внимание, что точка запрещена. Это необходимо, чтобы неожиданные кейсы писались в _log.warning.
    result = re.match(r"([А-Яа-яёЁa-zA-Z +,/\-\d]+)", name)
    if not (result is None):
        if name == result[0]:
            parsed_name["subject"] = result[1]
            return parsed_name

    # '15.10-18.50 МЕЖФАКУЛЬТЕТСКИЕ КУРСЫ'
    # Этот кейс вынесен отдельно по такой же причине, как и запрет точки в прошлом кейсе.
    result = re.match(r"(15\.10-18\.50 МЕЖФАКУЛЬТЕТСКИЕ КУРСЫ)", name)
    if not (result is None):
        if name == result[0]:
            parsed_name["subject"] = result[1]
            return parsed_name

    # Неожиданные кейсы - это, например, '429 - С/К по выбору доц. Водовозов В. Ю.'. Из-за 'доц.'
    # все ломается и либо все будет записано в 'subject', либо препод запарсится, а 'доц.' будет в 'subject'.
    # Поэтому такие кейсы надо писать в _log.warning.

    # '... доц. Водовозов В. Ю.'
    result = re.match(r"([А-Яа-яёЁa-zA-Z +,/\-\d]+) (доц. [А-Яа-яёЁa-zA-Z]+ [А-Яа-яёЁa-zA-Z]\. [А-Яа-яёЁa-zA-Z]\.)", name)
    if not (result is None):
        if name == result[0]:
            parsed_name["subject"] = result[1]
            parsed_name["teacher"] = result[2]
            return parsed_name

    _logger.warning(f"Для '{name}' не найдено подходящее регулярное выражение.")
    return {"subject": name, "teacher": None, "place": None}


def parse_name(lessons):
    """
    Разделяет колонку 'name' на 'subject', 'teacher' и 'place'.
    """
    _logger.info("Начинаю парсить 'name'...")

    parsed_names = []
    for index, row in lessons.iterrows():
        name = row["name"]
        parsed_name = _parse_name(name)
        parsed_names.append(parsed_name)

    lessons = lessons.reset_index(drop=True)
    addition = pd.DataFrame(parsed_names).reset_index(drop=True)
    lessons = pd.concat([lessons, addition], axis=1)
    lessons.drop("name", inplace=True, axis=1)
    return lessons


if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("s")
    args = parser.parse_args()
    print(_parse_name(args.s))
