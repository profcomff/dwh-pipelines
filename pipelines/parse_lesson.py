from sqlalchemy import create_engine
import pandas as pd
import re


def parse_lesson(url):
    engine = create_engine(url)

    lessons = pd.read_sql_table("lessons", engine)
    decode_patterns = pd.read_sql_table("decode_patterns", engine)
    decode_patterns = decode_patterns.sort_values(by="priority")

    parsed_names = []
    for index, row in lessons.iterrows():
        name = row["name"]
        # Нельзя просто ставить None, иначе если везде будет только None, будут неправильные колонки.
        parsed_name = {"subject": None, "teacher": None, "place": None}

        for regex in decode_patterns["regex"]:
            results = re.match(regex, name)
            if results is None:
                continue
            else:
                parsed_name = results.groupdict()
                break
        parsed_names.append(parsed_name)

    lessons = pd.concat([lessons, pd.DataFrame(parsed_names)], axis=1)
    lessons.drop("name", inplace=True, axis=1)
    with engine.begin() as connection:
        lessons.to_sql("parsed_lessons_temporary_table", con=connection, if_exists='replace', index=False)

    # # Этот код создает "базу" для таблицы с историей. Это нужно, чтобы быстро передать типы колонок.
    # lessons = lessons.query("index == -1")
    # with engine.begin() as connection:
    #     lessons.to_sql("lessons_with_history", con=connection, if_exists='replace', index=False)
    # with engine.connect() as connection:
    #     connection.execute("""
    #         ALTER TABLE lessons_with_history ADD is_deleted integer NULL;
    #         ALTER TABLE lessons_with_history ADD create_ts information_schema."time_stamp" NULL DEFAULT now();
    #         """)
