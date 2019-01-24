import requests as rq
from bs4 import BeautifulSoup
import settings
import time 
import json 
import regex as re
import numpy as np
import datetime

def collect_cars(car_urls):
    """ Takes a list of urls and gets car and seller information, storing it in a list-wise JSON object"""
    cars=[]
    sellers=[]
    # Regex compiler to strip unicode spaces # 
    regex=re.compile(r'[\n\r\t]')

    for i, url in enumerate(car_urls):    
        r=rq.get(url)
        print(url)

        while r.status_code!=200:
            print('Bad Response, trying again in 1 minute')
            time.sleep(60)
            r=rq.get(url)

        soup=BeautifulSoup(r.text,'lxml')
        ### Get the tables !!! ###
        tables=soup.find_all('table',attrs={'cellspacing':'0','cellpadding':'5','width':'100%'}) 

        ### Car table first ### 

        try:
            car_info=tables[0]
        except:
            print('No car information found, possible expired listing')
            cars.append(None)
            sellers.append(None)
            continue

        # Extract all the table rows # 
        car_table_rows=tables[0].find_all('tr')
        # Find all the table entries # 
        table_entries=[row.find_all('td')[col].text for row in car_table_rows for col in range(len(row.find_all('td')))]
        # Take out the labels # 
        car_labels=[row.find('td',class_='label').text for row in car_table_rows if row.find('td',class_='label')!=None]
        
        
        # Extract values by taking the entry adjacent to the label # 
        table_values=[table_entries[table_entries.index(label)+1] for label in car_labels]
        # Clean up unicode characters and empty spaces # 
        car_feats=[regex.sub(" ", s).strip(' ') for s in table_values]
        # Get other features, image links, listing title and brand of car # 
        img_links=list(set([i.get('src') for i in soup.find_all('img') if i.get('src').split('/')[3]=='cars_used' and i.get('src').split('/')[4] != 'sold_tag.gif']))
        list_title=soup.find_all('a',class_="link_redbanner")[0].text
        brand=soup.find_all('a',class_="link_redbanner")[0].text.split(' ')[0]
        date_of_listing=datetime.datetime.now()

        # Add this to feature list, and add the labels too # 
        car_labels+= ['img_links','list_title','brand','date_of_listing']
        car_feats += [img_links,list_title,brand,date_of_listing]

        # Zip it up and append # 
        car_dict=dict(zip(car_labels,car_feats))
        cars.append(car_dict)

        ### Seller table next ### 

        try: 
            seller_info=tables[1]
        except: 
            print('No seller information found, possible expired listing')
            sellers.append(None)
            continue 

        # Get all rows in the seller table # 
        seller_table_rows=tables[1].find_all('tr')

        # Find all the table entries # 
        seller_table_entries=[row.find_all('td')[col].text for row in seller_table_rows for col in range(len(row.find_all('td')))]
        # Take out the seller labels # 
        seller_labels=[row.find('td',class_='sellerlabel').text for row in seller_table_rows if row.find('td',class_='sellerlabel')!=None]
        # Take out seller values by taking adjacent entry on the table# 
        seller_table_values=[seller_table_entries[seller_table_entries.index(label)+1] for label in seller_labels]

        # Zip, append # 
        seller_dict=dict(zip(seller_labels,seller_table_values))
        sellers.append(seller_dict)

        # Cache every 2500 results # 

        if i % 2500 == 0:
            print('Caching {} results...'.format(i))

            with open('./data/car_data_{}.json'.format(i),'w') as outfile:
                json.dump(cars,outfile)
            with open('./data/seller_data_{}.json'.format(i),'w') as outfile2:
                json.dump(sellers,outfile2) 

    return cars, sellers


if __name__ == '__main__':

    with open ('./data/listing_data.json','r') as file:
        d=json.load(file)

    cars,sellers=collect_cars(d['query_urls'])

    with open('./data/car_data_full.json','w') as outfile:
        json.dump(cars,outfile)
    with open('./data/seller_data_full.json','w') as outfile2:
        json.dump(sellers,outfile2)   
