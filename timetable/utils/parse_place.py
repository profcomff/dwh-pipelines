import logging

# Словарь для переименования.
_rename_place = {"Ауд. им. Хохлова": "ЦФА"}

_logger = logging.getLogger(__name__)


def parse_place(lessons):
    """
    На данный момент только меняет название 'place', если оно есть в словаре изменения названий.
    Дополнительно возвращает список кабинетов.
    """
    _logger.info("Начинаю парсить 'place'...")

    places = lessons["place"].tolist()
    for i, place in enumerate(places):
        place = _rename_place.get(place, place)
        places[i] = place

    lessons["place"] = places
    return lessons, list(set(places))
