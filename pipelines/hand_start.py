from pipelines.parse_lesson import parse_lesson
from pipelines.parse_timetable import parse_timetable
from pipelines.save_in_history_table import save_in_history_table


def hand_start(url):
    parse_timetable(url)
    parse_lesson(url)
    save_in_history_table(url)


hand_start("")

