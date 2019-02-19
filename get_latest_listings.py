import requests as rq
from bs4 import BeautifulSoup
import settings
import time 
import json 
import re
import datetime
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from scrape_cars import * 
from scrape_listings import * 
from private import *



### SQL Queries ### 
get_current_listings="""
                    
                    SELECT *
                    FROM listings

                    """


if __name__== '__main__':


    ## Open connection to DB # 

    def connect():
        
        return psycopg2.connect(host=DB_URL,database="sgcarmart", user=DB_USER, password=DB_PASSWORD)

    engine = create_engine('postgresql://',creator=connect)

    # Collect latest listings # 

    listings=collect_main_pages(settings.CAR_TYPES)

    updated_listings=pd.DataFrame(listings)

    # Get all current listings # 

    current_listings=pd.read_sql(get_current_listings,engine)

    # Check which are the new listings # 

    new_listings=updated_listings[-updated_listings['list_id'].isin(current_listings['list_id'])]

    print('{} new listings collected'.format(new_listings.shape[0]))

    # Write into DB # 
    new_listings.to_sql('listings',engine,if_exists='append',index=False)

    new_listings.reset_index(inplace=True)

    new_cars,new_sellers=collect_cars(new_listings['query_url'])

    clean_cars_dat=parse_cars(new_cars)
    clean_sellers_dat=parse_sellers(new_sellers)
    
    dealer_df=gen_dealer_df(clean_sellers_dat,new_listings)
    dealer_df.to_sql('dealers',engine,if_exists='append',index=False)

    car_df=gen_car_df(clean_cars_dat,new_listings)
    car_df.to_sql('cars',engine,if_exists='append',index=False)
    
    sales_df=gen_sales_df(clean_sellers_dat,new_listings)
    sales_df[sales_df['person_id']!=0].to_sql('sales',engine,if_exists='append',index=False)


    
   
    
    





