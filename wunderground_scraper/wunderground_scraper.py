import requests
from bs4 import BeautifulSoup
import datetime
from dateutil import parser
import re


class WundergroundScraper(object):

    def __init__(self, city='KBFI'):
        self.city = city
        self.url = 'http://www.wunderground.com/history/airport/'\
                   '{a}/{y}/{m}/{d}/DailyHistory.html'
        self.need_header = True
        self.quotes = '"{0}"'

    def download_date_range(self, start_dt, end_dt, f_path='data/weather.csv'):
        end_dt = parser.parse(end_dt)
        start_dt = parser.parse(start_dt)
        diff = end_dt - start_dt
        dates = [end_dt - datetime.timedelta(days=x) \
                          for x in xrange(1, diff.days)]
        for d in dates:
            table = self._make_request(d.year, d.month, d.day)
            if self.need_header:
                self._write_header(table, f_path)
                self.need_header = False
            self._write_data(str(d.date()), table, f_path)
        print '{0} downloaded.'.format(f_path)

    def _make_request(self, year, month, day):
        url = self.url.format(a=self.city, y=year, m=month, d=day)
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        table = soup.findAll('div', {'id': 'observations_details'})
        return table[0]

    def _write_header(self, table, f_path):
        data = ['"date"']
        for header in table.findAll('th'):
            for h in header.strings:
                if h != ('(PDT)'):
                    data.append(self.quotes.format(h.strip()))
        self._write_line(data, f_path)

    def _write_data(self, date, table, f_path):
        for row in table.findAll('tr', {'class': 'no-metars'}):
            data = [self.quotes.format(date)]
            for col in row.findAll('td'):
                content = col.text.strip('\n').strip().encode('utf-8')
                data.append(self.quotes.format(content))
            self._write_line(data, f_path)

    def _write_line(self, data, f_path):
        if self.need_header:
            w_type = 'w'
        else:
            w_type = 'a'

        with open(f_path, w_type) as f:
            f.write(','.join(data))
            f.write('\n')
