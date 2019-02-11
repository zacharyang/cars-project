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

def collect_cars(car_urls):
    """ Takes a list of urls and gets car and seller information, storing it in a list of dictionaries"""
    cars=[]
    sellers=[]
    # Regex compiler to strip unicode spaces # 
    regex=re.compile(r'[\n\r\t]')

    session = rq.Session()
    retry = Retry(connect=10, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    session.mount('http://', adapter)
    session.mount('https://', adapter)

    for i, url in enumerate(car_urls): 
        r=session.get(url)
        print(url)
        print('{:2.2f}% of records scraped'.format(i*100/len(car_urls)))

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
        car_labs=[row.find('td',class_='label').text for row in car_table_rows if row.find('td',class_='label')!=None]
        
        # Extract values by taking the entry adjacent to the label # 
        table_values=[table_entries[table_entries.index(label)+1] for label in car_labs]
        
        # Convert to lower case and take out column labels # 
        car_labels=[re.sub(r'[ ]','_',i.replace('.','').lower()) for i in car_labs]
        
        # Clean up unicode characters and empty spaces # 
        car_feats=[regex.sub(" ", s).strip(' ') for s in table_values]
        
        # Get other features, image links, listing title and brand of car # 
        img_links=list(set([i.get('src') for i in soup.find('div',attrs={'id':'gallery_holder'}).find_all('img')]))
        list_title=soup.find_all('a',class_="link_redbanner")[0].text
        brand=soup.find_all('a',class_="link_redbanner")[0].text.split(' ')[0]
        model= re.sub(r'\([^)]*\)', '',list_title).strip(' ')

        ## Clean up Date features # 
        date_scraped=datetime.datetime.now().strftime('%d-%m-%Y')
        date_str=unicodedata.normalize('NFKD',car_info.find('div',attrs={'id':'usedcar_postdate'}).text)
        date_posted=datetime.datetime.strptime(date_str.split(' ')[2], '%d-%b-%Y')
        date_updated=datetime.datetime.strptime(date_str.split(' ')[9], '%d-%b-%Y')
        car_tags=car_info.find('div',attrs={'id':'cartags'}).text.strip('Tags: ').replace(' ','')

        # Add this to feature list, and add the labels too # 
        car_labels+= ['img_links','list_title','model','brand','date_scraped','date_posted','date_updated','car_tags']
        car_feats += [img_links,list_title,model,brand,date_scraped,date_posted,date_updated,car_tags]

        # Zip it up and append # 
        car_dict=dict(zip(car_labels,car_feats))

        if car_dict['availability']=='SOLD':
            car_dict['days_to_sell']=(date_updated-date_posted).days 
        else:
            car_dict['days_to_sell']=np.nan
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
        seller_labs=[row.find('td',class_='sellerlabel').text for row in seller_table_rows if row.find('td',class_='sellerlabel')!=None]
        
        # Take out seller values by taking adjacent entry on the table# 
        seller_table_values=[seller_table_entries[seller_table_entries.index(label)+1] for label in seller_labs]
        
        # Convert to lower case and clean up column labels # 
        seller_labels=[re.sub(r'[ ]','_',i.replace('.','').lower()) for i in seller_labs]


        # Zip, append # 
        seller_dict=dict(zip(seller_labels,seller_table_values))
        sellers.append(seller_dict)

    return cars, sellers


def parse_cars(cars):

    ''' Parser to clean up column names and features. Listings that are expired/do not exist will be 
    filled with NaN values across the columns '''

    # Initialise a list of columns from the first entry # 
    car_cols=cars[0].keys()

    # Fill missing entries with an empty dictionary # 

    miss_cars=[i for i, x in enumerate(cars) if x == None]

    for i in miss_cars:
        cars[i]=dict(zip(car_cols,[np.nan for i in car_cols]))
    
    # Find the maximum number of cols in the whole set # 
    car_cols=set().union(*(d.keys() for d in cars))

    # separate out into two types of features to parse, dates and numeric # 
    date_cols=['org_reg_date', 'reg_date']
    numeric_cols=['arf', 'coe', 'curb_weight', 'depreciation', 'dereg_value', 'engine_cap', 'mileage', 'no_of_owners', 'omv', 'power', 'price', 'road_tax']

    for i, car in enumerate(cars):
        # Go through the list of maximum car features found across all listings # 
        for col in car_cols:
            # If the feature is not present in the listings, set to nan # 
            if col not in car.keys():
                car[col]=np.nan
            # If there's a dash or N.A, set to nan # 
            if car[col]=='-' or car[col]=='N.A.':
                car[col]=np.nan
            # Parse out the Power and mileage features # 
            if col == 'power'and car[col] is not np.nan or col=='mileage' and car[col] is not np.nan:
                car[col]=re.split("[()]",car[col])[0]
            # For numeric features, clean up and convert to float # 
            if col in numeric_cols and car[col] is not np.nan:
                car[col]=float(re.sub("[^0-9]", "",car[col]))
            # Convert to date time object for date cols # 
            if col in date_cols and car[col] is not np.nan:
                car[col]=datetime.datetime.strptime(re.split('[()]',car[col])[0],'%d-%b-%Y')


    return cars

def parse_sellers(sellers):

    ''' Parser function for the dealer information, which includes information about the dealer and the respective salespeople.

    '''

    # Initialise a list of column features # 
    seller_cols=sellers[0].keys()

    # Fill missing entries with an empty dictionary # 

    miss_sellers=[i for i, x in enumerate(sellers) if x == None]

    for i in miss_sellers:
        sellers[i]=dict(zip(seller_cols,[np.nan for i in seller_cols]))
    
    # Define the full feature set # 

    seller_cols=set().union(*(d.keys() for d in sellers))

    for i, seller in enumerate(sellers):

        for col in seller_cols:

            # Fill missing values with nans #
            if col not in seller.keys():
                seller[col]=np.nan
            if seller[col]=='-' or seller[col]=='N.A.':
                seller[col]=np.nan

            # Extract company name # 

            if col=='company' and seller[col] is not np.nan:
                # Some unicode in the company title # 
                company_feats=unicodedata.normalize('NFKD',seller[col]).split('» ')

                # Clean up the company title # 
                seller[col]=company_feats[0].strip(' ')

                # Some sellers might not have vehicle numbers, so have to try and get the 
                try:
                    n_vehicles=company_feats[2]
                    seller['sold_vehicles']=float(re.findall('\d+',n_vehicles)[0])
                    seller['available_vehicles']=float(re.findall('\d+',n_vehicles)[1])

                except IndexError:
                    pass

            # Clean up address # 
            
            if col=='address' and seller[col] is not np.nan:
                seller[col]=seller[col].replace('Search cars nearby this location','').strip(' ')

    return sellers

def gen_sales_df(sellers,listing_data):

    ''' Puts the salespersons data into a dataframe'''


    # Initialise a dictionary # 
    sales={
        'salesperson_name':[],
        'person_id':[],
        'dealer_id':[],
    }

    # Iterate through sellers data and extract information about salespersons # 

    for i,seller in enumerate(sellers):
        
        if seller['contact_person(s)'] is not np.nan:
            person_ids=re.findall('\d+',seller['contact_person(s)'])
            names=re.findall('[ a-zA-Z]+',seller['contact_person(s)'])
        

            if len(person_ids)==len(names):
                sales['salesperson_name']+=names
                sales['person_id']+=person_ids
                sales['dealer_id']+=[listing_data['dealer_id'][i] for j in names]
            else:
                continue
    
    sales_df=pd.DataFrame(sales).drop_duplicates()

    return sales_df


def gen_dealer_df(dealers,listing_data):

    ''' Generates a dataframe for all the dealers'''

    dealer_df=pd.DataFrame(dealers)
    dealer_df['dealer_id']=listing_data['dealer_id'][:dealer_df.shape[0]]

    # Only keeping these features about the dealers # 
    to_keep=['dealer_id','address','company','available_vehicles','sold_vehicles']


    return dealer_df[to_keep].drop_duplicates()

def gen_car_df(cars,listing_data):
    
    cars_df=pd.DataFrame(cars)
    cars_df['list_id']=listing_data['list_id'][:cars_df.shape[0]]

    return cars_df

if __name__ == '__main__':

    with open ('./data/master/listing_data_master.json','r') as file:
        l=json.load(file)
        file.close()

    listing_data=pd.DataFrame(l).drop_duplicates().reset_index()

    cars_dat,sellers_dat=collect_cars(listing_data['query_url'])

    clean_cars_dat=parse_cars(cars_dat)

    clean_sellers_dat=parse_sellers(sellers_dat)


    dealer_df=gen_dealer_df(clean_sellers_dat,listing_data)
    dealer_df.to_csv('./data/dealer.csv',index=False)

    car_df=gen_car_df(clean_cars_dat,listing_data)
    car_df.to_csv('./data/cars.csv',index=False)
    
    sales_df=gen_sales_df(clean_sellers_dat,listing_data)
    sales_df.to_csv('./data/sales.csv',index=False)
    
   
    
    

