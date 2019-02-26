import numpy as np
import pandas as pd
import psycopg2
import re
from sqlalchemy import create_engine
from sqlalchemy.sql import text
from sklearn.linear_model import LinearRegression
from sklearn.externals import joblib
from sklearn.ensemble import RandomForestRegressor, GradientBoostingRegressor
from sklearn.ensemble import RandomForestClassifier
import datetime
from dateutil.relativedelta import relativedelta
from private import * 


###===================================================================== Connect to DB, Get data =====================================================================###

engine = create_engine('postgresql://',creator=connect)

def get_cars_df():


    query="""
    SELECT *
    FROM cars 
    WHERE model in (SELECT DISTINCT model
                    FROM cars
                    WHERE model in (SELECT model
                                    FROM cars
                                    GROUP BY model
                                    HAVING COUNT(list_id)>10)
                    AND availability ='SOLD'
                    AND manufactured>2000 
                    AND price IS NOT NULL AND price>5 
                    AND reg_date IS NOT NULL
                    AND opc_scheme IS NULL
                    AND arf IS NOT NULL)
    AND manufactured>2000 
    AND price IS NOT NULL AND price>5 
    AND reg_date IS NOT NULL
    AND opc_scheme IS NULL
    AND arf IS NOT NULL


    """

    cars=pd.read_sql_query(query,engine)
    cars.reset_index(drop=True,inplace=True)
    cars.drop(columns=['index','level_0'],inplace=True)

    return cars

def get_coe_df():

    coe_query='''
            SELECT coe_my_cat,value
            FROM coe_long
            '''
    coe_long=pd.read_sql_query(coe_query,engine)

    coe_fill_5=coe_long
    coe_fill_5['value']=coe_long['value']/2

    return coe_long, coe_fill_5

def get_sold_df(cars):

    sold=cars[cars['availability']=='SOLD']

    dealer_query='''
            SELECT DISTINCT list_id, l.dealer_id , d.company 
            FROM listings AS l
            LEFT JOIN (SELECT DISTINCT dealer_id, company, available_vehicles, sold_vehicles 
                        FROM dealers) as d on l.dealer_id= d.dealer_id
            WHERE company IS NOT NULL

    '''
    dealers=pd.read_sql_query(dealer_query,engine)

    sell=pd.merge(sold,dealers,how='left',on='list_id')

    sold10=sell['company'].isin(sell['company'].value_counts()[sell['company'].value_counts()>10].index.to_list())
    companies_sub=sell[['days_to_sell','company']][sold10].groupby(by='company').mean().sort_values(by='days_to_sell')
    top_dealers=companies_sub[companies_sub['days_to_sell']<=7].index.to_list()
    top_dealer=sell['company'].isin(top_dealers)
    sell['is_top_dealer']=top_dealer.astype(int)

    return sell


###==================================================================== Helper functions ===========================================================================###


def coe_expiry(x):
    
    if x.has_5yr_COE:
        return x.date_posted + relativedelta(years=5)
    elif x.has_10yr_COE:
        return x.date_posted + relativedelta(years=10)
    elif x.has_COE_date:
        return datetime.datetime.strptime(re.search('\((.*?)\)',x.list_title).group(1).split('till ')[1],'%m/%Y')
    else: 
        return x.reg_date + relativedelta(years=10)


def coe_my_update(x):

    if x.has_5yr_COE or x.has_10yr_COE:
        return x.date_posted.strftime('%b-%Y')
    elif x.has_COE_date:
        new_dt=x.reg_date + relativedelta(years=10)
        return new_dt.strftime('%b-%Y')
    else:
        return x.reg_date.strftime('%b-%Y')


def road_tax_calculator(x):
    base=0
    if x.engine_cap<=600:
        base=400*0.782
    if 600 < x.engine_cap <=1000:
        base=(400 + 0.25*(x.engine_cap - 600))*0.782
    if 1000 < x.engine_cap <= 1600:
        base=(500 + 0.75*(x.engine_cap - 1000))*0.782
    if 1600 < x.engine_cap <= 3000:
        base=(950 + 1.5*(x.engine_cap - 1600))*0.782
    if x.engine_cap>3000:
        base=(3050 + 2*(x.engine_cap-3000))*0.782
        
    if x.veh_age>10:
        return base*(1+0.1*(x.veh_age-10))
    else:
        return base

def parf_calculator(x):
    if x.is_coe_car==1:
        return 0
    if x.veh_age<5:
        return 0.75*(x.arf)
    if 5<=x.veh_age<10:
        return (0.75-0.05*(x.veh_age-4))*(x.arf)
    if x.veh_age>=10:
        return 0


def coe_rebate_calculator(x):
    if x.has_5yr_COE:
        return x.coe*x.month_to_coe_expiry/60
    else:
        return x.coe*x.month_to_coe_expiry/120



###=========================================================== Data Cleaning, Fill Missing Values ==================================================================###

def clean_cars(cars):

    cars[['reg_date','date_posted','date_updated']]=cars[['reg_date','date_posted','date_updated']].applymap(lambda x:datetime.datetime.strptime(x.split()[0],'%Y-%m-%d'))
    cars['date_scraped']=cars['date_scraped'].apply(lambda x:datetime.datetime.strptime(x.split()[0],'%d-%m-%Y'))

    cars['power']=cars[['power','brand']].groupby(cars['brand']).transform(lambda x: x.fillna(x.mean()))
    cars['depreciation']=cars[['depreciation','brand']].groupby(cars['brand']).transform(lambda x: x.fillna(x.mean()))
    cars['coe_cat']=cars['engine_cap'].apply(lambda x: 'CAT_A' if x <1600. else 'CAT_B')
    cars['curb_weight']=cars[['curb_weight','type_of_veh']].groupby(cars['type_of_veh']).transform(lambda x: x.fillna(x.mean()))
    cars['veh_age']=cars['date_posted'].dt.year-cars['reg_date'].dt.year
    cars['veh_age_mths']=cars.date_posted.dt.to_period('M')-cars.reg_date.dt.to_period('M')
    cars['veh_age_mths']=cars['veh_age_mths'].apply(lambda x: x.n)

    car_type_dummies=cars['category'].str.get_dummies(sep=', ')
    labels=car_type_dummies.columns
    car_type_dummies.columns=['is_'+x.lower().replace(' ','_') for x in labels]
    cars=pd.concat([cars,car_type_dummies],axis=1)

    cars['has_5yr_COE']=cars['list_title'].str.contains('New 5-yr COE')
    cars['has_10yr_COE']=cars['list_title'].str.contains('New 10-yr COE')
    cars['has_COE_date']=cars['list_title'].str.contains('COE till')
    cars['has_renewed_COE']=cars['has_5yr_COE']| cars['has_10yr_COE']|cars['has_COE_date']

    cars['coe_expiry_date']=cars.apply(lambda x:coe_expiry(x),axis=1)

    cars['month_to_coe_expiry']=cars.coe_expiry_date.dt.to_period('M') - cars.date_posted.dt.to_period('M')
    cars['month_to_coe_expiry']=cars['month_to_coe_expiry'].apply(lambda x: x.n)
    cars['reg_my']=cars.apply(lambda x: coe_my_update(x),axis=1)
    cars['coe_my_cat']=cars['reg_my']+'-'+cars['coe_cat']


    coe_miss_5=pd.merge(cars[['coe_my_cat','coe']][(cars['coe'].isna()) & cars['has_5yr_COE']],coe_fill_5,on='coe_my_cat',right_index=True)
    cars['coe'].fillna(value=coe_miss_5['value'],inplace=True)

    coe_miss_10=pd.merge(cars[['coe_my_cat','coe']][cars['coe'].isna()],coe_long,on='coe_my_cat',right_index=True)
    cars['coe'].fillna(value=coe_miss_10['value'],inplace=True)


    cars['road_tax'].fillna(cars[['engine_cap','veh_age']][cars['road_tax'].isna()].apply(lambda x: road_tax_calculator(x),axis=1),inplace=True)

    cars['coe_rebate']=cars.apply(lambda x: coe_rebate_calculator(x),axis=1)

    cars['parf_rebate']=cars[['veh_age','arf','is_coe_car']].apply(lambda x: parf_calculator(x),axis=1)
    cars['dereg_value'].fillna(cars['parf_rebate'][cars['dereg_value'].isna()]+cars['coe_rebate'][cars['dereg_value'].isna()],inplace=True)

    cars['no_of_owners'].fillna(1,inplace=True)

    cars['value']=cars['price']-cars['dereg_value']

    cars=cars[cars['value']>0].reset_index(drop=True)

    return cars

def impute_mileage(cars):

    m=cars['mileage'][cars['mileage'].notna()]
    age_lm=cars['veh_age_mths'][cars['mileage'].notna()]*cars['is_low_mileage_car'][cars['mileage'].notna()]
    X=pd.concat([cars[['veh_age_mths','is_low_mileage_car']][cars['mileage'].notna()],age_lm],axis=1).values


    X_miss=cars[['veh_age_mths','is_low_mileage_car']][cars['mileage'].isna()]
    X_miss['interaction']=cars['veh_age_mths'][cars['mileage'].isna()]*cars['is_low_mileage_car'][cars['mileage'].isna()]

    lr_impute=LinearRegression().fit(X,m)

    mileage_miss=pd.Series(lr_impute.predict(X_miss),index=cars['mileage'][cars['mileage'].isna()].index)

    cars['mileage'].fillna(mileage_miss,inplace=True)

    return cars


class Trainer:

    def __init__(self,df):
        self.df=df
        self.brand_dums=pd.get_dummies(df['brand'],drop_first=True)
        self.model_dums=pd.get_dummies(df['model'],drop_first=True)

    def train_omv(self,features):

        df=self.df

        X=pd.concat([df[features],self.brand_dums],axis=1)
        y= df['omv']
        omv_model=joblib.load('./flask/OMV.pkl')
        omv_model.fit(X,y)
        self.omv_model = omv_model

    def train_price(self,features):
        
        df=self.df
        X=pd.concat([df[features],self.model_dums],axis=1)
        y= df['value']
        price_model=joblib.load('./flask/PRICE.pkl')
        price_model.fit(X,y)
        price_model.n_features_

        preds=price_model.predict(X)
        resids=y-preds
        self.price_model = price_model
        self.covariance_mat= np.linalg.inv(np.dot(X.T,X))
        self.residual_error= np.sum([resid**2 for resid in resids])/(X.shape[0]-X.shape[1]) 

    def train_proba(self,features):

        df=self.df
        X=pd.concat([df[features],self.model_dums],axis=1)
        y=df['days_to_sell']<=7
        y=y.astype(int)

        proba_model=joblib.load('./flask/PROBA.pkl')
        proba_model.fit(X,y)

        self.proba_model = proba_model

def train_main():

    omv_features=['engine_cap','power','curb_weight']

    price_features=['engine_cap','power','curb_weight','omv','mileage','veh_age_mths','month_to_coe_expiry']

    price= Trainer(cars)

    price.train_omv(omv_features)

    price.train_price(price_features)

    return price



def train_dashboard():

    proba_features=['price','value','engine_cap','power','curb_weight','omv','mileage','veh_age_mths','month_to_coe_expiry',
         'has_renewed_COE','is_premium_ad_car','is_direct_owner_sale','is_consignment_car','is_top_dealer']

    prob= Trainer(sell)

    prob.train_proba(proba_features)

    return prob


if __name__ == '__main__':

    cars = get_cars_df()

    coe_long ,coe_fill_5=get_coe_df()

    cars= clean_cars(cars)

    cars= impute_mileage(cars)

    price=train_main()


    with open ('./flask/OMV.pkl','wb') as outfile:
        joblib.dump(price.omv_model,outfile)
    
    with open ('./flask/PRICE.pkl','wb') as outfile:
        joblib.dump(price.price_model,outfile)

    np.save('./flask/cov.npy',price.covariance_mat)
    np.save('./flask/residual_error.npy',price.residual_error)

    sell=get_sold_df(cars)

    proba=train_dashboard()

    with open ('./flask/PROBA.pkl','wb') as outfile:
        joblib.dump(proba.proba_model,outfile)













