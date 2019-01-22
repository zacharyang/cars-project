import requests as rq
from bs4 import BeautifulSoup
import settings
import time 
import json 
import regex as re


def collect_main_pages(car_types):


    ## Initialise the dictionary to store the car information ## 

    car_dat= { 'list_queries':[],
                'list_ids':[],
                'dealer_ids':[],
                'query_urls':[],

    }
    for car in car_types:
        # Get 100 results per page # 
        counter=0
        r=rq.get('https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&VEH={}'.format(counter,settings.CAR_REF[car]))
        while r.status_code != 200:
            print('Bad Response. Trying again in 1 minute.')
            time.sleep(60)
            r=rq.get('https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&VEH={}'.format(counter,settings.CAR_REF[car]))
        print('{} {}s collected'.format(counter,car))

        soup=BeautifulSoup(r.text,'lxml')
        pagebar=soup.find_all(name='span',attrs={'class':'pagebar'})

        query_url = 'https://www.sgcarmart.com/used_cars/'

        while len(pagebar)>0:
            url='https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&VEH={}'.format(counter,settings.CAR_REF[car])
            r=rq.get(url)
            while r.status_code != 200:
                print('Bad Response. Trying again in 1 minute.')
                time.sleep(60)
                r=rq.get(url)
            
            soup=BeautifulSoup(r.text,'lxml')
            pagebar=soup.find_all(name='span',attrs={'class':'pagebar'})

            # Find all links in bold # 
            strong=soup.find_all(name='strong')
            list_queries=[x.a.get('href') for x in strong if x.a != None and x.a.get('href')[:11]=='info.php?ID']
            car_dat['list_queries']+=list_queries
            car_dat['list_ids']+=[re.split(r"[^a-zA-Z0-9\s]",x)[3] for x in list_queries]
            car_dat['dealer_ids']+=[re.split(r"[^a-zA-Z0-9\s]",x)[5] for x in list_queries]
            car_dat['query_urls']+=[query_url+x for x in list_queries]
            
            counter+=100
            print(url)

    return car_dat


if __name__== '__main__':

    listing_data=collect_main_pages(settings.CAR_TYPES)

    with open('./data/listing_data.json', 'w') as outfile:
        json.dump(listing_data, outfile)
















