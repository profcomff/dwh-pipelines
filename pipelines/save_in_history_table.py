import history_table.core as history_table


def save_in_history_table(url):
    DATA_COLUMNS = 'start, "end", odd, even, weekday, num, "group", subject, teacher, place'
    history_table.supply_data(url, "lessons_with_history", "parsed_lessons_temporary_table", DATA_COLUMNS)
