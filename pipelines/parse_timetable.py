import requests
import pandas as pd
from sqlalchemy import create_engine
import timetable.timetable_parser as parser


def parse_timetable(url):
    USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
    HEADERS = {"User-Agent": USER_AGENT}

    results = pd.DataFrame()
    sources = [[1, 1, 1]]  # [[1, 1, 6], [1, 2, 6], [1, 3, 6], [2, 1, 6], [2, 2, 6], [2, 3, 6], [3, 1, 10], [3, 2, 8],
    # [4, 1, 10], [4, 2, 8], [5, 1, 13], [5, 2, 10], [6, 1, 10], [6, 2, 9]]
    for source in sources:
        for group in range(1, source[2]+1):
            html = requests.get('http://ras.phys.msu.ru/table/{year}/{stream}/{group}.htm'
                                .format(year=source[0], stream=source[1], group=group), headers=HEADERS).content
            results = pd.concat([results, pd.DataFrame(parser.run(html))])
    results["id"] = results.apply(lambda x: hash(tuple(x)), axis=1)

    engine = create_engine(url)
    with engine.begin() as connection:
        results.to_sql("lessons", con=connection, if_exists='replace', index=False)
