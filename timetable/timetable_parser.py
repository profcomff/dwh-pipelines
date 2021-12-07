import requests
from bs4 import BeautifulSoup


USER_AGENT = "Mozilla/5.0 (Linux; Android 7.0; SM-G930V Build/NRD90M) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/59.0.3071.125 Mobile Safari/537.36"
HEADERS = {"User-Agent": USER_AGENT}


class Group:
    def __init__(self, number, link):
        self.number = number
        self.days = []

        page = requests.get(link, headers=HEADERS)
        soup = BeautifulSoup(page.content, "html.parser")
        body = soup.select("body")[0]
        tags = body.find_all("tr")[3:-1]

        weekday = 0
        tags_for_day = []
        for tag in tags:
            if len(tag.select("td.delimiter")) != 0:
                self.days.append(Day(weekday, tags_for_day))
                tags_for_day = []
                continue

            tags_for_day.append(tag)

    def getData(self):
        data = []
        for day in self.days:
            for lesson in day.lessons:
                data.append({"group": self.number, "day": day.weekday, "number": lesson.number,
                             "start": lesson.start, "end": lesson.end, "up": lesson.up,
                             "bottom": lesson.bottom, "html": lesson.html})
        return data


class Day:
    def __init__(self, weekday, tags):
        self.weekday = weekday
        self.lessons = []

        number = -1
        for tag in tags:
            lesson = Lesson(tag)

            if lesson.start is None:
                lesson.start = self.lessons[-1].start
                lesson.end = self.lessons[-1].end
            else:
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
        self.html = " "

        if len(html.select("td.tditem1")) != 0 and len(html.select("td.tdsmall0")) == 0:
            self.parseItem1(html)

        if len(html.select("td.tdsmall1")) != 0:
            self.parseSmall1(html)

        if len(html.select("td.tdsmall0")) != 0:
            self.parseSmall0(html)

    def parseItem1(self, html):
        self.html = html.select("td.tditem1")[0].get_text().replace("\xa0", " ")
        self.parseTime(html.select("td.tdtime")[0])

    def parseSmall1(self, html):
        self.html = html.select("td.tdsmall1")[0].get_text().replace("\xa0", " ")
        html_time = html.select("td.tdtime")
        if len(html_time) != 0:
            self.bottom = False
            self.parseTime(html_time[0])
        else:
            self.up = False

    def parseSmall0(self, html):
        html_time = html.select("td.tdtime")
        if len(html_time) != 0:
            self.parseTime(html_time[0])

            items = html.select("td.tdsmall0")
            self.html = ""
            for item in items:
                self.html += item.get_text().replace("\xa0", " ")+" | "
            self.html = self.html[:-3]

    def parseTime(self, html):
        time = html.contents
        self.start = time[0]
        self.end = time[-1]


group = Group(101, "http://ras.phys.msu.ru/table/1/1/1.htm")
data = group.getData()
print(data)


