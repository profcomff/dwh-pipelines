import requests
import pandas as pd
from sqlalchemy import create_engine
import timetable.timetable_parser as parser
import hashlib


def parse_timetable(url):
    USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
    HEADERS = {"User-Agent": USER_AGENT}
    engine = create_engine(url)

    results = pd.DataFrame()
    sources = pd.read_sql_table("sources", engine)
    # sources = pd.DataFrame([[2, 1, 2]])
    for index, source in sources.iterrows():
        for group in range(1, source[2]+1):
            html = requests.get('http://ras.phys.msu.ru/table/{year}/{stream}/{group}.htm'
                                .format(year=source[0], stream=source[1], group=group), headers=HEADERS).content
            results = pd.concat([results, pd.DataFrame(parser.run(html))])
    results["id"] = results.apply(lambda x: hashlib.md5("".join([i.__str__() for i in x]).encode()).hexdigest(), axis=1)

    engine = create_engine(url)
    with engine.begin() as connection:
        results.to_sql("lessons", con=connection, if_exists='replace', index=False)