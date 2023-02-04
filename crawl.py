from datetime import date, timedelta
import requests
import json
import csv
import pandas as pd

# a dump request to init header of csv file
r = requests.get(
    "https://api.weather.com/v1/location/KDCA:9:US/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&startDate=20221117&endDate=20221117")
d = json.loads(r.text)
print(type(d['observations']))
csv_columns = d['observations'][0].keys()
row = d['observations'][0].values()
print(csv_columns)

with open('dict.csv', 'a', newline='') as csv_file:
    writer = csv.writer(csv_file)
    writer.writerow(csv_columns)

from datetime import date
from dateutil.rrule import rrule, DAILY

a = date(2022, 1, 1)
b = date(2022, 11, 17)
csv_file = "Names.csv"
base_request = 'https://api.weather.com/v1/location/KDCA:9:US/observations/historical.json?apiKey=e1f10a1e78da46f5b10a1e78da96f525&units=e&'
for dt in rrule(DAILY, dtstart=a, until=b):
    date = dt.strftime("%Y%m%d")
    new_request = base_request + 'startDate=' + date + '&endDate=' + date
    r = requests.get(new_request)
    data = json.loads(r.text)
    with open('dict.csv', 'a', newline='') as csv_file:
        writer = csv.writer(csv_file)
        for peroid in data['observations']:
            new_row = peroid.values()
            writer.writerow(new_row)
