import logging

import pandas as pd

_logger = logging.getLogger(__name__)


def _to_list(value):
    if isinstance(value, list):
        result = []
        for item in value:
            result += _to_list(item)
        return result

    if value is None:
        return []

    if pd.isna(value):
        return []

    return [value]


def flatten(lessons):
    """
    Единственное, что делает эта функция: это превращает каждый элемент из 'place' и 'teacher' в list.
    """
    _logger.info("Произвожу сглаживание...")

    lessons["place"] = lessons["place"].map(_to_list)
    lessons["teacher"] = lessons["teacher"].map(_to_list)

    return lessons





