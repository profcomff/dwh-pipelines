import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}

# Типы пар в расписании.
# Имена переменных связаны с названием классов в самом html.
KIND_ITEM1 = 0
KIND_SMALL1_WITH_TIME = 1
KIND_SMALL1_WITHOUT_TIME = 2
KIND_ITEM1_WITH_SMALL0 = 3
KIND_SMALL1_WITH_SMALL0_WITH_TIME = 4
KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME = 5


class GroupParser:
    def __init__(self, html):
        html = html.select("body")[0].select("tr")[1]
        self.tags = list(filter(lambda x: x != "\n", html.children))

    def parse_name(self):
        return self.tags[1].select("b")[-1]

    def get_days(self):
        days = []

        weekday = 0
        tags = []
        for tag in self.tags[3:]:
            if len(tag.select("td.delimiter")) != 0:
                days.append({"weekday": weekday, "tags": tags})
                weekday += 1
                tags = []
                continue

            tags.append(tag)

        return days


class DayParser:
    def __init__(self, day):
        self.weekday = day["weekday"]
        self.tags = day["tags"]

    def get_lessons(self):
        lessons = []
        for tag in self.tags:
            lessons.append({"weekday": self.weekday, "html": tag})
        return lessons


class LessonParser:
    def __init__(self, html):
        self.html = html

    def define_kind(self):
        if len(self.html.select("td.tditem1")) != 0:
            if len(self.html.select("td.tdsmall0")) != 0:
                return KIND_ITEM1_WITH_SMALL0
            return KIND_ITEM1

        if len(self.html.select("td.tdsmall1")) != 0:
            if len(self.html.select("td.tdsmall0")) != 0:
                if len(self.html.select("td.tdtime")) != 0:
                    return KIND_SMALL1_WITH_SMALL0_WITH_TIME
                else:
                    return KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME
            else:
                if len(self.html.select("td.tdtime")) != 0:
                    return KIND_SMALL1_WITH_TIME
                else:
                    return KIND_SMALL1_WITHOUT_TIME


    # ------------ Имена методов связано с названием классов в самом html. ------------
    def parse_as_item1(self):
        html = self.html.select("td.tditem1")[0]
        time = self.html.select("td.tdtime")[0].contents
        return {"html": html, "start": time[0], "end": time[-1], "up": True, "bottom": True}


    def parse_as_small1_with_time(self):
        html = self.html.select("td.tdsmall1")[0]
        time = self.html.select("td.tdtime")[0].contents
        return {"html": html, "start": time[0], "end": time[-1], "up": True, "bottom": False}


    def parse_as_small1_without_time(self):
        html = self.html.select("td.tdsmall1")[0]
        return {"html": html, "up": False, "bottom": True}


    def parse_as_item1_with_small0(self):
        html = self.html.select("td.tdsmall0")[0]
        time = self.html.select("td.tdtime")[0].contents
        return {"html": html, "start": time[0], "end": time[-1], "up": True, "bottom": True}


    def parse_as_small1_with_small0_with_time(self):
        html = self.html.select("td.tdsmall0")[0]
        time = self.html.select("td.tdtime")[0].contents
        return {"html": html, "start": time[0], "end": time[-1], "up": True, "bottom": False}


    def parse_as_small1_with_small0_without_time(self):
        html = self.html.select("td.tdsmall0")[0]
        time = self.html.select("td.tdtime")[0].contents
        return {"html": html, "start": time[0], "end": time[-1], "up": False, "bottom": True}


def run(url):
    page = requests.get(url, headers=HEADERS)
    html = BeautifulSoup(page.content, "html.parser")

    groupParser = GroupParser(html)
    name = groupParser.parse_name()
    days = groupParser.get_days()

    lessons = []
    for day in days:
        number = -1
        for lesson in DayParser(day).get_lessons():
            lessonParser = LessonParser(lesson["html"])
            kind = lessonParser.define_kind()

            # ------------ Имена переменных связаны с названием классов в самом html. ------------
            if kind == KIND_ITEM1:
                lesson.update(lessonParser.parse_as_item1())

            if kind == KIND_SMALL1_WITH_TIME:
                lesson.update(lessonParser.parse_as_small1_with_time())

            if kind == KIND_SMALL1_WITHOUT_TIME:
                lesson.update(lessonParser.parse_as_small1_without_time())
                number -= 1
                lesson.update({"start": lessons[-1]["start"], "end": lessons[-1]["end"]})

            if kind == KIND_ITEM1_WITH_SMALL0:
                lesson.update(lessonParser.parse_as_item1_with_small0())

            if kind == KIND_SMALL1_WITH_SMALL0_WITH_TIME:
                lesson.update(lessonParser.parse_as_small1_with_small0_with_time())

            if kind == KIND_SMALL1_WITH_SMALL0_WITHOUT_TIME:
                lesson.update(lessonParser.parse_as_small1_with_small0_without_time())
                number -= 1
                lesson.update({"start": lessons[-1]["start"], "end": lessons[-1]["end"]})


            number += 1
            lesson.update({"number": number})
            lesson.update({"group": name})

            lessons.append(lesson)

    return lessons


if __name__ == '__main__':
    data = run("http://ras.phys.msu.ru/table/1/1/1.htm")
    print(data)


