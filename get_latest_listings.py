import requests as rq
from bs4 import BeautifulSoup
import settings
import time 
import json 
import regex as re
import datetime

def collect_latest_listings(car_types):


    ## Initialise the dictionary to store the car information ## 

    car_dat= { 'list_queries':[],
                'list_ids':[],
                'dealer_ids':[],
                'query_urls':[],

            }
    for car in car_types:
        # Get 100 results per page # 
        counter=0
        url='https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&AVL=2&VEH={}'.format(counter,settings.CAR_REF[car])
        r=rq.get(url)
        while r.status_code != 200:
            print('Bad Response. Trying again in 1 minute.')
            time.sleep(60)
            r=rq.get(url)
        

        soup=BeautifulSoup(r.text,'lxml')
        pagebar=soup.find_all(name='span',attrs={'class':'pagebar'})

        query_url = 'https://www.sgcarmart.com/used_cars/'

        while len(pagebar)>0:
            r=rq.get(url)
            print(url)
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
            url='https://www.sgcarmart.com/used_cars/listing.php?BRSR={}&RPG=100&AVL=2&VEH={}'.format(counter,settings.CAR_REF[car])
        print('{} {} collected'.format(counter,car))

    return car_dat


def update_master(listings,master):

    keep_ind=[i for i,a in enumerate(listings['query_urls']) if a not in master['query_urls']]
    if len(keep_ind)==0:
        print('No new listings for {}'.format(datetime.datetime.now()))
        return None, None
    else:
        print('Adding {} new listings to master data'.format(len(keep_ind)))
        new_master={}
        new_listings={}
        for k in listings.keys():
            new_listings[k]=[listings[k][i] for i in keep_ind]
            new_master[k]=master[k]+listings[k]

    return new_listings, new_master


if __name__== '__main__':

    listing_data=collect_latest_listings(settings.CAR_TYPES)

    with open('./data/master/listing_data_master.json', 'r') as file:
        master=json.load(file)
        file.close()

    new_listings,updated_master=update_master(listing_data,master)

    if updated_master is not None:

        with open('./data/master/listing_data_master.json', 'w') as outfile:
            json.dump(updated_master,outfile)
        with open('./data/cache/new_listings_{}-{}.json'.format(datetime.datetime.now().day,datetime.datetime.now().month),'w') as outfile:
            json.dump(new_listings,outfile)
    else:
        print('No new listings, master not updated.')

  


