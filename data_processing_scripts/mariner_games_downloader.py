import urllib2
import imp
import os


CWD = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_PATH = CWD + '/../data/upcoming_mariner_games.csv'
URL = url = 'http://mlb.mlb.com/ticketing-client/csv/EventTicketPromotionPrice.tiksrv?team_id=136&display_in=singlegame&ticket_category=Tickets&site_section=Default&sub_category=Default&leave_empty_games=true&event_type=T&event_type=Y'

def download_upcoming_mariner_games(file_path=DOWNLOAD_PATH):
    response = urllib2.urlopen(URL)
    with open(file_path, 'w') as f:
        for line in response:
            f.write(line)
    print 'File downloaded.'