import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

# Типы пар в расписании.
# Имена переменных связано с названием классов в самом html.
KIND_ITEM1 = 0
KIND_SMALL1_WITH_TIME = 1
KIND_SMALL1_WITHOUT_TIME = 2
KIND_ITEM1_WITH_SMALL0 = 3
KIND_SMALL1_WITH_SMALL0_WITH_TIME = 4
KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME = 5


class Group:
    def __init__(self, html):
        self.days = []
        self.raw_html = html
        self.name = ""


    def parse(self):
        html = self.raw_html.select("body")[0].select("tr")[1]
        tags = list(filter(lambda x: x != "\n", html.children))

        tags_for_group = tags[1].select("b")[-1].contents[::2]
        for tag in tags_for_group:
            self.name += tag + " | "
        self.name = self.name[:-3]

        day = Day(0)
        tags_for_day = []
        for tag in tags[3:]:
            if len(tag.select("td.delimiter")) != 0:
                day.parse(tags_for_day)
                self.days.append(day)

                day = Day(day.weekday+1)
                tags_for_day = []

                continue

            tags_for_day.append(tag)


    def get_data(self):
        data = []
        for day in self.days:
            for lesson in day.lessons:
                data.append({"group": self.name, "day": day.weekday, "number": lesson.number,
                             "start": lesson.start, "end": lesson.end, "up": lesson.up,
                             "bottom": lesson.bottom, "html": lesson.html})
        return data


class Day:
    def __init__(self, weekday):
        self.weekday = weekday
        self.lessons = []


    def parse(self, tags):
        number = -1
        for tag in tags:
            lesson = Lesson(tag)
            kind = lesson.define_kind()

            if kind == KIND_ITEM1:
                lesson.parse_as_item1()

            if kind == KIND_SMALL1_WITH_TIME:
                lesson.parse_as_small1_with_time()

            if kind == KIND_SMALL1_WITHOUT_TIME:
                lesson.parse_as_small1_without_time()
                lesson.start = self.lessons[-1].start
                lesson.end = self.lessons[-1].end
                number -= 1

            if kind == KIND_ITEM1_WITH_SMALL0:
                lesson.parse_as_item1_with_small0()

            if kind == KIND_SMALL1_WITH_SMALL0_WITH_TIME:
                lesson.parse_as_small1_with_small0_with_time()

            if kind == KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME:
                lesson.parse_as_small1_with_small0_without_time()
                lesson.start = self.lessons[-1].start
                lesson.end = self.lessons[-1].end
                number -= 1

            number += 1
            lesson.number = number

            if lesson.html != " ":
                self.lessons.append(lesson)


class Lesson:
    def __init__(self, html):
        self.number = None
        self.up = True
        self.bottom = True
        self.start = None
        self.end = None
        self.raw_html = html
        self.html = " "


    def define_kind(self):
        if len(self.raw_html.select("td.tditem1")) != 0:
            if len(self.raw_html.select("td.tdsmall0")) != 0:
                return KIND_ITEM1_WITH_SMALL0
            return KIND_ITEM1

        if len(self.raw_html.select("td.tdsmall1")) != 0:
            if len(self.raw_html.select("td.tdsmall0")) != 0:
                if len(self.raw_html.select("td.tdtime")) != 0:
                    return KIND_SMALL1_WITH_SMALL0_WITH_TIME
                else:
                    return KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME
            else:
                if len(self.raw_html.select("td.tdtime")) != 0:
                    return KIND_SMALL1_WITH_TIME
                else:
                    return KIND_SMALL1_WITHOUT_TIME

    def parse_time(self, html):
        time = html.contents
        self.start = time[0]
        self.end = time[-1]


    # ------------ Имена методов связано с названием классов в самом html. ------------

    def parse_as_item1(self):
        self.html = self.raw_html.select("td.tditem1")[0].get_text().replace("\xa0", " ")
        self.parse_time(self.raw_html.select("td.tdtime")[0])


    def parse_as_small1_with_time(self):
        self.html = self.raw_html.select("td.tdsmall1")[0].get_text().replace("\xa0", " ")
        self.parse_time(self.raw_html.select("td.tdtime")[0])
        self.bottom = False


    def parse_as_small1_without_time(self):
        self.html = self.raw_html.select("td.tdsmall1")[0].get_text().replace("\xa0", " ")
        self.up = False


    def parse_as_item1_with_small0(self):
        self.parse_time(self.raw_html.select("td.tdtime")[0])

        tags = self.raw_html.select("td.tdsmall0")
        self.html = ""
        for tag in tags:
            self.html += tag.get_text().replace("\xa0", " ")+" | "
        self.html = self.html[:-3]


    def parse_as_small1_with_small0_with_time(self):
        self.parse_time(self.raw_html.select("td.tdtime")[0])
        self.bottom = False

        tags = self.raw_html.select("td.tdsmall0")
        self.html = ""
        for tag in tags:
            self.html += tag.get_text().replace("\xa0", " ") + " | "
        self.html = self.html[:-3]


    def parse_as_small1_with_small0_without_time(self):
        self.up = False

        tags = self.raw_html.select("td.tdsmall0")
        self.html = ""
        for tag in tags:
            self.html += tag.get_text().replace("\xa0", " ") + " | "
        self.html = self.html[:-3]


def run(url):
    page = requests.get(url, headers=HEADERS)
    group = Group(BeautifulSoup(page.content, "html.parser"))
    group.parse()
    return group.get_data()


if __name__ == '__main__':
    data = run("http://ras.phys.msu.ru/table/4/1/7.htm")
    print(data)


