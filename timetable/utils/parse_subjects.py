import logging
import re

_logger = logging.getLogger(__name__)


def _preprocessing(subject):
    """По сути, исправление опечаток в названии пар."""
    if subject == "105М, 106М, 110М, 141М - 105МДМП, 106М, 141М, 110М с/к по выб ":
        return "105М, 106М, 110М, 141М - 105М ДМП, 106М, 141М, 110М с/к по выб "

    if subject == "216Ма, 221М - 216мА ДМП,С/К по выбору 221м ":
        return "216Ма, 221М - 216мА ДМП,221м - С/К по выбору "

    return subject


def _compare_groups(group1, group2):
    """Сравнение групп. Этот процесс сложнее, чем '=='."""
    group2 = group2.replace(" ", "")
    group2 = group2.lower()

    # Возможна ситуация названия пары 307а - ... у 307 группы (3 курс).
    result = re.match(r"\d{3}\D", group2)
    if result is not None:
        return group1 in group2

    return group1 == group2


def _parse_subjects(group, subject):
    """
    Парсит 'subjects' по заданным регулярным выражениям. Возвращает subject, если у группы есть такой предмет,
    в противном случае возвращает None.
    В случае отсутствия подходящего регулярного выражения выдает предупреждение и возвращает сам 'subject'.
    """
    number_group = r"\d{3} {0,1}[А-Яа-яёЁ]*"
    name_subject = r"[А-Яа-яёЁA-Z ./\-]+"
    delimiter = r"[, .и+\-]*"

    subject = _preprocessing(subject)

    # 307{n} - ...
    for i in range(12):
        result = re.match(f"({number_group})" + f"{delimiter}({number_group})" * i + f" *-* ({name_subject})", subject)
        if not (result is None):
            if subject == result[0]:
                if not any([_compare_groups(group, result[1 + j]) for j in range(i + 1)]):
                    return None
                else:
                    return result[1 + i + 1]

    # 307{n} - ..., 302{m} - ...
    for i in range(8):
        for j in range(8):
            result = re.match(f"({number_group})" + f"{delimiter}({number_group})" * i + f" *-* ({name_subject}), *" +
                              f"({number_group})" + f"{delimiter}({number_group})" * j + f" *-* ({name_subject})",
                              subject)
            if not (result is None):
                if subject == result[0]:
                    left = any([_compare_groups(group, result[1 + k]) for k in range(i + 1)])
                    right = any([_compare_groups(group, result[1 + i + 1 + 1 + k]) for k in range(j + 1)])

                    if left:
                        return result[1 + i + 1]
                    elif right:
                        return result[1 + i + 1 + 1 + j + 1]
                    else:
                        return None

    # 1 поток без 307 группы - S
    result = re.match(f"1 поток без [34]07 группы - ({name_subject})", subject)
    if not (result is None):
        if subject == result[0]:
            if any([_compare_groups(group, _group) for _group in ["307", "407"]]):
                return None
            else:
                return result[1]

    # 1 поток без 307 группы и астр. - S
    result = re.match(rf"1 поток без [34]07 группы,* *и астр\.* - ({name_subject})", subject)
    if not (result is None):
        if subject == result[0]:
            if any([_compare_groups(group, _group) for _group in ["307", "407", "301", "401"]]):
                return None
            else:
                return result[1]

    # 4 курс без астр, и 407 - S
    result = re.match(rf"[34] курс без астр\.*,* *и [34]07 - ({name_subject})", subject)
    if not (result is None):
        if subject == result[0]:
            if any([_compare_groups(group, _group) for _group in ["307", "407", "301", "401"]]):
                return None
            else:
                return result[1]

    # Механика
    result = re.match(f"({name_subject})", subject)
    if not (result is None):
        if subject == result[0]:
            return result[1]

    # 15.10-18.50 МЕЖФАКУЛЬТЕТСКИЕ КУРСЫ
    result = re.match(r"(15\.10-18\.50 МЕЖФАКУЛЬТЕТСКИЕ КУРСЫ)", subject)
    if not (result is None):
        if subject == result[0]:
            return result[1]

    _logger.warning(f"Для '{subject}' не найдено подходящее регулярное выражение.")
    return subject


def parse_subjects(lessons):
    """
    Парсит колонку 'subject' и, если надо, удаляет строчку из таблицы (если в названии предметы указаны группы).
    Дополнительно возвращает список предметов.
    """
    _logger.info("Начинаю парсить 'subjects'...")

    subjects = []
    deleted_rows = []
    for index, row in lessons.iterrows():
        subject = row["subject"]

        subject = _parse_subjects(row["group"], subject)
        if subject is None:
            deleted_rows.append(index)
        else:
            subjects.append(subject)

    lessons.drop(deleted_rows, axis=0, inplace=True)
    lessons["subject"] = subjects
    lessons = lessons.reset_index()

    return lessons, list(set(lessons["subject"].tolist()))
