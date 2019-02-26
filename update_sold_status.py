import requests as rq
from bs4 import BeautifulSoup
import settings
import time 
import json 
import re
import numpy as np
import datetime
import unicodedata
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
from scrape_cars import *
from scrape_listings import * 
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from private import *

## SQL Queries # 

available_cars='''
SELECT query_url, l.list_id
FROM listings l
LEFT JOIN cars c
ON l.list_id=c.list_id
WHERE c.availability ='Available'
'''


update=text("""
    UPDATE cars
    SET (availability, date_posted,date_scraped,date_updated,days_to_sell) = (:availability, :date_posted,:date_scraped,:date_updated,:days_to_sell)
    WHERE list_id=:list_id
    """)

def update_sold_status(update_df):

    session = rq.Session()
    retry = Retry(connect=10, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    urls=update_df['query_url']
    list_ids=update_df['list_id']
    for i,url in enumerate(urls):
        r=session.get(url)

        while r.status_code!=200:
            print('Bad Response, trying again in 1 minute')
            time.sleep(60)
            r=rq.get(url)

        soup=BeautifulSoup(r.text,'lxml')
        ### Get the tables !!! ###
        tables=soup.find_all('table',attrs={'cellspacing':'0','cellpadding':'5','width':'100%'}) 

        try:
            car_info=tables[0]
        except:
            print('No car information found, possible expired listing')
            continue

        date_scraped=datetime.datetime.now().strftime('%d-%m-%Y')

        try:
            date_str=unicodedata.normalize('NFKD',car_info.find('div',attrs={'id':'usedcar_postdate'}).text)
        except:
            print('No date data found')
            continue

        date_posted=datetime.datetime.strptime(date_str.split(' ')[2], '%d-%b-%Y')
        date_updated=datetime.datetime.strptime(date_str.split(' ')[9], '%d-%b-%Y')
        days_to_sell=(date_updated-date_posted).days 
        list_id=list_ids[i]

        data={'availability':'SOLD','date_posted':date_posted,'date_scraped':date_scraped,\
        'date_updated':date_updated,'days_to_sell':days_to_sell,'list_id':list_id}
        
        print('Updating records for {}'.format(url))
        engine.execute(update,data)
        print('{:2.2f}% of available cars status updated to database'.format(i*100/len(urls)))
        

if __name__ == '__main__':


    engine = create_engine('postgresql://',creator=connect)



    available_car_urls=pd.read_sql(available_cars,engine)
    sold_cars=collect_main_pages(settings.CAR_TYPES,get_sold=True)

    to_be_updated=available_car_urls[available_car_urls['query_url'].isin(sold_cars['query_url'])].reset_index()

    update_sold_status(to_be_updated)




