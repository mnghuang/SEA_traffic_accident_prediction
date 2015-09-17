import requests
from bs4 import BeautifulSoup
import datetime
from dateutil import parser
import re
import pandas as pd


class WundergroundScraper(object):

    def __init__(self, city='KBFI'):
        self.city = city
        self.url = 'http://www.wunderground.com/history/airport/'\
                   '{a}/{y}/{m}/{d}/DailyHistory.html'
        self.data = []

    def download_date_range(self, start_dt, end_dt, f_path='data/weather.csv'):
        end_dt = parser.parse(end_dt)
        start_dt = parser.parse(start_dt)
        diff = end_dt - start_dt
        dates = [end_dt - datetime.timedelta(days=x) \
                          for x in xrange(1, diff.days)]
        for d in dates:
            table = self._make_request(d.year, d.month, d.day)
            header = self._get_header(table)
            self._write_data(str(d.date()), table, header)
        self._save_to_csv(f_path)
        

    def _make_request(self, year, month, day):
        url = self.url.format(a=self.city, y=year, m=month, d=day)
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        table = soup.findAll('div', {'id': 'observations_details'})
        return table[0]

    def _get_header(self, table):
        data = ['date']
        for header in table.findAll('th'):
            for h in header.strings:
                if '(' not in h:
                    data.append(h.strip())
        return data

    def _write_data(self, date, table, header):
        for row in table.findAll('tr', {'class': 'no-metars'}):
            data = [date]
            for col in row.findAll('td'):
                content = col.text.strip('\n').strip().encode('utf-8')
                data.append(content)
            self._data_to_dict(header, data)

    def _data_to_dict(self, header, row):
        d = dict()
        for i, h in enumerate(header):
            d[h] = row[i]
        self.data.append(d) 

    def _save_to_csv(self, f_path):
        df = pd.DataFrame(self.data)
        df.to_csv(f_path, index=False)
        print '{0} downloaded.'.format(f_path)
