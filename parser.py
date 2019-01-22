import json
import settings 
import re 
import pandas as pd
import numpy as np
import datetime 

def parse_cars(cars):

    car_cols=cars[0].keys()

    miss_cars=[i for i, x in enumerate(cars) if x == None]

    for i in miss_cars:
        cars[i]=dict(zip(car_cols,[np.nan for i in car_cols]))
        
    car_cols=set().union(*(d.keys() for d in cars))
    date_cols=['Org. Reg Date','Reg Date']
    numeric_cols=['ARF','COE','Curb Weight','Depreciation','Dereg Value','Engine Cap','Mileage','No. of Owners','OMV','Power','Price','Road Tax']
    for i, car in enumerate(cars):
        for col in car_cols:
            if col not in car.keys():
                car[col]=np.nan
            if car[col]=='-' or car[col]=='N.A.':
                car[col]=np.nan
            if col == 'Power'and car[col] is not np.nan or col=='Mileage' and car[col] is not np.nan:
                car[col]=re.split("[()]",car[col])[0]
            if col in numeric_cols and car[col] is not np.nan:
                car[col]=float(re.sub("[^0-9]", "",car[col]))
            if col in date_cols and car[col] is not np.nan:
                car[col]=datetime.datetime.strptime(re.split('[()]',car[col])[0],'%d-%b-%Y')

    return cars

def parse_sellers(sellers):
    seller_cols=sellers[0].keys()

    miss_sellers=[i for i, x in enumerate(sellers) if x == None]
    
    for i in miss_sellers:
        sellers[i]=dict(zip(seller_cols,[np.nan for i in seller_cols]))
        
    seller_cols=set().union(*(d.keys() for d in sellers))
    for i, seller in enumerate(sellers):
        for col in seller_cols:
            if col not in seller.keys():
                seller[col]=np.nan
            if seller[col]=='-' or seller[col]=='N.A.':
                seller[col]=np.nan
            if col=='Company' and seller[col] is not np.nan:
                company_feats=seller[col].split('»')
                seller[col]=(company_feats[0].encode('ascii','ignore')).decode("utf-8")
                try:
                    n_vehicles=company_feats[2]
                    seller['Sold Vehicles']=float(re.findall('\d+',n_vehicles)[0])
                    seller['Available Vehicles']=float(re.findall('\d+',n_vehicles)[0])
                except IndexError:
                    pass
            if col == 'Contact Person(s)' and seller[col] is not np.nan:
                seller["Num Sellers"]=len(re.findall('\d+',seller[col]))
        
    return sellers


if __name__ == '__main__':

    with open ('./data/master/car_data_master.json','r') as file:
        cars=json.load(file)
        file.close()

    clean_cars=parse_cars(cars)

    car_df=pd.DataFrame(clean_cars)

    with open ('./data/master/seller_data_master.json','r') as file:
        sellers=json.load(file)
        file.close()

    clean_sellers=parse_sellers(sellers)

    seller_df=pd.DataFrame(clean_sellers)

    with open ('./data/master/listing_data_master.json','r') as file:
        listings=json.load(file)
        file.close()

    listings_df=pd.DataFrame(listings)

    main_df=pd.concat([listings_df,car_df,seller_df],axis=1)

    main_df.to_csv('./data/clean/main.csv',index=False)




            