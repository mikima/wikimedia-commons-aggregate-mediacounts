import requests
from datetime import date, datetime, timedelta
from dateutil.relativedelta import relativedelta
from pprint import pprint
import csv

def getMonthlyViews(year,month):
    # example:
    # https://wikimedia.org/api/rest_v1/metrics/mediarequests/top/en.wikipedia.org/image/2022/05/all-days
    baseurl = 'https://wikimedia.org/api/rest_v1/metrics/mediarequests/top/en.wikipedia.org/image/'+year+'/'+month+'/all-days'
    r = requests.get(baseurl)
    data = r.json()
    return data


start_date = date(2021, 10, 1) 
end_date = date(2022, 10, 30)

dict = {}

while start_date <= end_date:
    checkdate = date(start_date.year, start_date.month, 1)
    
    
    print(checkdate.strftime('%Y'),checkdate.strftime('%m'))
    data = getMonthlyViews(checkdate.strftime('%Y'),checkdate.strftime('%m'))

    for file in data['items'][0]['files']:

        if file['file_path'] in dict:
            dict[file['file_path']] = dict[file['file_path']] + file['requests']
        else:
            dict[file['file_path']] = file['requests']
        # dict[file['file_path']]
    start_date += relativedelta(months=+1)

pprint(dict)
ordered = [[k,v] for k,v in dict.items()]

sorted = sorted(ordered, key=lambda x: x[1], reverse=True)
pprint(sorted)

with open('tests/top1000year.csv', 'w') as outfile:
    # create the csv writer
    writer = csv.writer(outfile)
    # write the headers
    writer.writerow(['title','requests'])
    for row in sorted:
        writer.writerow(row)