import logging

import pandas as pd

_logger = logging.getLogger(__name__)


def multiple_lessons(lessons):
    """
    Соединяет пары, у которых одинаковые ['weekday', 'group', 'subject', 'start'], в одну строчку.
    """
    _logger.info("Начинаю соединять одинаковые пары...")

    for (_, _, _, _), sub_df in lessons.groupby(['weekday', 'group', 'subject', 'start']):
        if len(sub_df) > 1:
            teachers = sub_df['teacher'].values
            places = sub_df['place'].values
            new_df = sub_df.iloc[0].copy()
            new_df['teacher'] = list(teachers)
            new_df['place'] = list(places)

            indexes = sub_df.index
            lessons.drop(indexes, axis=0, inplace=True)
            lessons.loc[len(lessons) + 1] = new_df

    indexes_for_new_df = pd.Series(list(range(len(lessons))))
    lessons.set_index(indexes_for_new_df, inplace=True)

    return lessons
