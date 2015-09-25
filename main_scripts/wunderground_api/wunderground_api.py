import requests
from bs4 import BeautifulSoup

FIELD_MAPPING = {'conditions': 'weather', 
				 'winddir': 'wind_dir', 
				 'dewpoint': 'dewpoint_f', 
				 'gustpeed': 'wind_gust_mph',
				 'heatindex': 'heat_index_f',
				 'humidity': 'relative_humidity',
				 'precip': 'precip_1hr_in',
				 'pressure': 'pressure_in',
				 'temp': 'temp_f',
				 'visibility': 'visibility_mi',
				 'windspeed': 'wind_mph'}

class WundergroundAPI(object):

	def __init__(self, api='3455d3f7e7c94122'):
		self.api = api
		self.url = 'http://api.wunderground.com/api/{a}/' \
				   'conditions/q/{s}/{c}.json'

	def get_current_condition(self, city='Seattle', state='WA'):
		url = self.url.format(a = self.api, s = state, c = city)
		r_json = requests.get(url).json()
		return self._extract_field_mappings(r_json['current_observation'])

	def _extract_field_mappings(self, r_json):
		weather = dict()
		for k, v in FIELD_MAPPING.iteritems():
			weather[k] = r_json[v]
		return weather
		
