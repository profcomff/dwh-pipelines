from sqlalchemy import create_engine
import pandas as pd
import re


def parse_name_lesson(url):
    engine = create_engine(url)

    lessons = pd.read_sql("select * from lessons", engine)
    lessons = pd.DataFrame(lessons, columns=lessons.columns)

    decode_patterns = pd.read_sql("select * from decode_patterns", engine)
    decode_patterns = pd.DataFrame(decode_patterns, columns=decode_patterns.columns)
    decode_patterns.sort_values("priority")

    parsed_names = []
    for index, row in lessons.iterrows():
        name = row["name"]
        parsed_name = {"subject": "", "teacher": "", "place": ""}

        for regex in decode_patterns["regex"]:
            results = re.match(regex, name)
            if results is None:
                continue
            else:
                parsed_name = results.groupdict()
                break
        parsed_names.append(parsed_name)

    lessons = pd.concat([lessons, pd.DataFrame(parsed_names)], axis=1)
    with engine.begin() as connection:
        lessons.to_sql("parsed_lessons", con=connection, if_exists='replace')

