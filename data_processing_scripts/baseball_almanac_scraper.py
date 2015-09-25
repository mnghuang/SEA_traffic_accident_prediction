import requests
from bs4 import BeautifulSoup


class BaseballAlmanacScraper(object):

    def __init__(self, team_city='SEA'):
        self.team_city = team_city
        self.url = 'http://www.baseball-almanac.com/teamstats/'\
                   'schedule.php?y={0}&t={1}'

    def download_year(self, year, file_path=None):
        if file_path is None:
            file_path = 'data/{0}_{1}.csv'.format(self.team_city, year)

        url = self.url.format(year, self.team_city)
        r = requests.get(url)
        soup = BeautifulSoup(r.text)
        table = soup.find('table', {'class': 'boxed'})
        self._write_csv(table, file_path)

    def download_years(self, years):
        for year in years:
            self.download_year(year)

    def _write_csv(self, table, file_path):
        with open(file_path, 'w') as f:
            for row in table.findAll('tr')[1:-4]:
                line = ['"{0}"'.format(s) for s in row.strings if s != '\n']
                f.write(','.join(line))
                f.write('\n')
