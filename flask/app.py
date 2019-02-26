from flask import Flask, render_template, url_for, request, session
from flask_session import Session
import psycopg2
from sqlalchemy import create_engine
from sqlalchemy.sql import text
import pandas as pd
import numpy as np
import json
from collections import defaultdict
from sklearn.externals import joblib
import datetime
from dateutil.relativedelta import relativedelta
from private import * 

###================================================================== Defining app Global variables ==============================================================###


# ### Connect to DB ## 

engine = create_engine('postgresql://',creator=connect)


### Get all models with availability = sold. Only models that have been sold before can be considered # 
all_model_query='''

            SELECT DISTINCT model, brand
            FROM cars
            WHERE model in (SELECT model
            FROM cars
            GROUP BY model
            HAVING COUNT(list_id)>10) 
            AND manufactured>2000 
            AND price IS NOT NULL AND price>5 
            AND reg_date IS NOT NULL
            AND opc_scheme IS NULL
            AND arf IS NOT NULL
            AND availability='SOLD'
            ORDER BY model ASC


            '''

all_models=engine.execute(all_model_query).fetchall()
all_models_df=pd.DataFrame(all_models,columns=['model','brand'])


def gen_model_list(all_models):

    '''
        Args: List of tuples in the form of (model, brand)

        Output: Hierarchical JSON object that generates drop down list for the select2 model drop down menu

        Eg. [{'text': 'brand1', children':{'id':'1','text':'brand1 model1',...}, {'text':'brand2','children':{.....}}]

    '''

    res = defaultdict(list)

    for v, k in all_models: 
        res[k].append(v)
    model_l=[{'text':k, 'children':v} for k,v in res.items()]
    i=0


    for brand in model_l:
        models=[]
        for model in brand['children']:
            d={}
            d['id']=i
            d['text']=model
            i+=1
            models.append(d)
        brand['children']=models

    return model_l

model_list= gen_model_list(all_models)


def gen_brand_dummies():
    '''
        Generates a list of brand dummies for OMV prediction model

    '''
    brand_query='''
                    SELECT DISTINCT brand
                    FROM cars
                    WHERE model in (SELECT model
                    FROM cars
                    GROUP BY model
                    HAVING COUNT(list_id)>10) 
                    AND manufactured>2000 
                    AND price IS NOT NULL AND price>5 
                    AND reg_date IS NOT NULL
                    AND opc_scheme IS NULL
                    AND arf IS NOT NULL
                    '''

    brands=[c[0] for c in engine.execute(brand_query).fetchall()]
    brands.remove('Toyota')

    return brands

brands = gen_brand_dummies()

def parf_calculator(arf,veh_age_mths):
    '''
        Generates PARF rebate for a vehicle 

        Args: arf: Vehicle ARF(int/float), veh_age_mths: Vehicle age in months (int)

        Output: PARF rebate (float)

    '''
    veh_age=np.floor(veh_age_mths/12)
    if veh_age<5:
        return 0.75*(arf)
    if 5<=veh_age<10:
        return (0.75-0.05*(veh_age-4))*(arf)
    if x.veh_age>=10:
        return 0

def arf_calculator(x):
    '''
        Generates ARF for vehicle based on OMV

        x: OMV of vehicle (int)

    '''

    arf=20000
    if x<=20000:
        return arf
    if 20000<x<=50000:
        return arf + 1.4*(x-arf)
    if x>50000:
        return arf + 1.4*(x-arf) +1.8*(x-50000)

def results_parser(results):

    '''
        Takes user input, generates the deregistration value and vehicle age in months and stores it in the original JSON object supplied

    '''

    # Get reg month and convert to date time, both in str format # 
    reg_month=str(int(results['reg_month'])+1)
    reg_year=results['reg_year']
    reg_my=reg_month+'-'+str(reg_year)
    reg_my_dt=datetime.datetime.strptime(reg_my,'%m-%Y')

    # Get vehicle age in months # 
    
    veh_age_mths=pd.Timestamp(datetime.datetime.now()).to_period('M')-pd.Timestamp(reg_my_dt).to_period('M')
    if type(veh_age_mths)!= int:
        veh_age_mths=veh_age_mths.n

    results['veh_age_mths'] = veh_age_mths


    # Get ARF # 
    arf=arf_calculator(int(results['omv']))

    # Get COE # 
    coe=int(results['coe'])


    # If car has extended COE, Set parf rebate = 0 and pro-rate COE rebate by number of months left to COE expiry # 
    if 'coe_extend' in results.keys():

        # Set coe expiry date to reg date + 10 + number of years renewed # 
        n_years_renewed=int(results['coe_extend'])
        coe_expiry_date=reg_my_dt+ datetime.timedelta(days=365.25*(n_years_renewed+10))


        month_to_coe_expiry=pd.Timestamp(coe_expiry_date).to_period('M')-pd.Timestamp(datetime.datetime.now()).to_period('M')
        if type(month_to_coe_expiry)!=int:
            month_to_coe_expiry=month_to_coe_expiry.n
        # Set parf = 0 #
        parf_rebate=0
        # Calculate coe rebate # 
        coe_rebate = coe*month_to_coe_expiry/ (n_years_renewed*12)

        # Store in results dict # 
        results['month_to_coe_expiry']=month_to_coe_expiry

        results['has_renewed_COE']= 1

    # If PARF car, calculate parf rebate and pro-rate COE rebate by number of months left to COE expiry # 
    else:

        # Set coe expiry date to reg date + 10 years # 
        coe_expiry_date=reg_my_dt + datetime.timedelta(days=365.25*10)
        month_to_coe_expiry=pd.Timestamp(coe_expiry_date).to_period('M')-pd.Timestamp(datetime.datetime.now()).to_period('M')
        if type(month_to_coe_expiry)!=int:
            month_to_coe_expiry=month_to_coe_expiry.n

        # Calculate parf # 
        parf_rebate=parf_calculator(arf,veh_age_mths)
        # Calculate coe rebate # 
        coe_rebate= coe * month_to_coe_expiry/120
        # Store in results dict # 
        results['month_to_coe_expiry']=month_to_coe_expiry

        results['has_renewed_COE']= 0

    # Calculate and store dereg value # 

    dereg_value = coe_rebate + parf_rebate
    results['dereg_value'] = dereg_value

    # Get car model# 
    model_id=int(results['model'])
    model=all_models_df.loc[model_id]['model']

    # Initialise model comparison list from index 1 onwards because get_dummies dropped first cat # 
    models=all_models_df['model'][1:]

    # Generate list to match model dummies # 
    model_v=[1 if m==model else 0 for m in models]

    results['model_v']=model_v


    return results


def price_predict(results):

    '''
        Takes user input and generated features to feed into price prediction model
    '''


    # Load price prediction model # 

    price_predictor=joblib.load('PRICE.pkl')

    # Feature list for query vehicle # 

    features_list= [ 'engine_cap', 'power', 'curb_weight', 'omv', 'mileage', 'veh_age_mths', 'month_to_coe_expiry']

    vals=np.array([float(results[i]) for i in features_list]+results['model_v']).reshape(1,-1)

    if vals.shape[1]!=price_predictor.n_features_:
        raise Exception('Incorrect number of features. Try re-training the model')



    value=int(np.round(price_predictor.predict(vals)[0]))

    results['value']=value
    price = value + int(results['dereg_value'])

    # Generate prediction interval based on linear model #  
    cov=np.load('cov.npy')
    e=np.load('residual_error.npy')
    var=e.reshape(1,)[0]

    pe=np.dot(np.dot(vals,cov),vals.T)*var + var
    prediction_error=pe[0][0]
    width=int(np.round(np.sqrt(prediction_error)*1.96))
    results['price_lower']=price-width
    results['price_upper']=price+width


    results['price']=price
    price = '{:,}'.format(price)

    return results, price

def pred_prob_sell(results):

    '''
        Takes user input from dashboard form and returns probability of selling vehicle within 7 days

    '''


    # Load probability prediction model # 

    predictor=joblib.load('PROBA.pkl')

    # Set up feature array # 

    features_list=['price','value','engine_cap','power','curb_weight','omv','mileage','veh_age_mths','month_to_coe_expiry',
         'has_renewed_COE','is_premium_ad_car','is_direct_owner_sale','is_consignment_car','is_top_dealer']

    for feature in features_list:
        # Set to zero if form does not have value # 
        if feature not in results.keys():
            results[feature]=0


    vals=np.array([results[i] for i in features_list]+results['model_v']).reshape(1,-1)

    if vals.shape[1]!=predictor.n_features_:
        raise Exception('Incorrect number of features. Try re-training the model')

    # Return probability # 

    proba=predictor.predict_proba(vals)

    
    return proba

def get_dashboard_metrics(results):

    '''
        Gets metrics to populate dashboard 

        Input: User form in JSON

        Ouput: 4 metrics - num_models: number (int) of models available for sale 
                           price_comp: number (int) of models in price range. Price range is determined by price_upper and price_lower in the user form
                           top_dealer: name (str) of company that has sold the most vehicles
                           top_dealer_sold: number (int) of models that the top dealer has sold

    '''

    if results['model']=='None':

        return None

    # Metric 1: num_models # 

    num_models_query=text('''

        SELECT COUNT(list_id)
        FROM (SELECT list_id, model 
              FROM cars WHERE 
              availability='Available' ) l 
        GROUP BY model
        HAVING (model) = (:model)

            ''')

    model_id=int(results['model'])
    model=all_models_df.loc[model_id]['model']
    num_models=engine.execute(num_models_query,{'model':model}).fetchall()[0][0]

    # Metric 2: price_comp # 

    price_comp_query=text('''

        SELECT COUNT(list_id)
        FROM cars
        WHERE (price) BETWEEN (:price_lower) AND (:price_upper)
        AND availability='Available'
            ''')

    price_comp=engine.execute(price_comp_query,{'price_lower':results['price_lower'],'price_upper':results['price_upper']}).fetchall()[0][0]

    # Metric 3: top_dealer, top_dealer_sold # 

    top_dealer_query=text('''
            SELECT company, COUNT(mdl.list_id) as n
            FROM (
            SELECT list_id, l.dealer_id, d.company
            FROM listings l
            INNER JOIN (SELECT DISTINCT dealer_id, company
                        FROM dealers
                        WHERE company IS NOT NULL) d
                        on l.dealer_id=d.dealer_id) dl 
            INNER JOIN (SELECT DISTINCT list_id, model
                        FROM cars
                        WHERE (model) = (:model)
                        AND availability = 'SOLD') mdl
                        on mdl.list_id=dl.list_id
            GROUP BY company
            ORDER BY n DESC
            LIMIT 1
        ''')

    qr=engine.execute(top_dealer_query,{'model':model}).fetchall()[0]
    top_dealer=qr[0]
    top_dealer_sold=qr[1]

    metrics ={
            'num_models':num_models,
            'price_comp':price_comp,
            'top_dealer':top_dealer,
            'top_dealer_sold':top_dealer_sold,

    }

    return metrics







###======================================================================================================================================================================###


###======================================================================== Initialising the app ========================================================================###




app = Flask(__name__)
SESSION_TYPE='filesystem'
app.config['SECRET_KEY']=SECRET_KEY
app.config.from_object(__name__)
Session(app)


# Set up defaults if visitor goes directly to dashboard endpoint # 
defaults= {'model': 'None'}


@app.route('/')
def home():
    return render_template('home.html',models=model_list)

@app.route('/autofill',methods=['POST'])
def autofill():

    autofill=request.get_json()
    model_id=int(autofill['model'])
    model=all_models_df.loc[model_id]['model']
    brand=all_models_df.loc[model_id]['brand']
    reg_month=autofill['reg_month']
    reg_year=autofill['reg_year']

    query=text('''

    SELECT engine_cap,power,curb_weight,model, brand
    FROM cars
    WHERE (model,brand) = (:model, :brand)

    ''')

    data={'model':model,'brand':brand}
    r=engine.execute(query,data)

    calc=pd.DataFrame(r.fetchall(),columns=['engine_cap','power','curb_weight','model','brand'])
    est_engine_cap=calc['engine_cap'].median()
    est_power=calc['power'].median()
    est_curb_weight=calc['curb_weight'].median()

    brand_v=[1 if b==brand else 0 for b in brands]


    omv_predictor=joblib.load('OMV.pkl')
    vals=np.array([est_engine_cap,est_power,est_curb_weight]+brand_v).reshape(1,-1)
    est_omv=omv_predictor.predict(vals)


    coe_cat=''
    if est_engine_cap<1600:
        coe_cat='CAT_A'
    else:
        coe_cat='CAT_B'

    reg_my=reg_month+'-'+str(reg_year)
    reg_my_dt=datetime.datetime.strptime(reg_my,'%b-%Y')

    if 'coe_extend' in autofill.keys():
        reg_my_dt=reg_my_dt+relativedelta(years=10)
        reg_my=reg_my_dt.strftime('%b-%Y')
        coe_extend=int(autofill['coe_extend'])
        print(coe_extend)
        
    coe_my_cat=reg_my+'-'+coe_cat

    coe_query=text('''
            
            SELECT value
            FROM coe_long
            WHERE (coe_my_cat) = (:coe_my_cat)


    ''')
    est_coe=engine.execute(coe_query,{'coe_my_cat':coe_my_cat}).fetchall()[0][0]

    if 'coe_extend' in autofill.keys() and coe_extend==5:
        est_coe=est_coe/2


    autofill_data={
        'est_engine_cap':est_engine_cap,
        'est_power':est_power,
        'est_curb_weight':est_curb_weight,
        'est_omv':int(np.round(est_omv[0])),
        'est_coe':int(np.round(est_coe)), 
    }
    return json.dumps(autofill_data)


@app.route('/results',methods=['POST'])
def results():

    results=request.form

    results=dict(results)

    user_data=results_parser(results)

    user_data, price = price_predict(user_data)

    session['user_data']=user_data

    return json.dumps({'price':price})


@app.route('/dashboard')
def dashboard():

    user_data=session.get('user_data',None)
    if user_data is None:
        user_data=defaults

    return render_template('dashboard.html',user_data=user_data, models=model_list)

@app.route('/dashboard/metrics',methods=['POST'])
def metrics():

    user_data=session.get('user_data',None)

    metrics=get_dashboard_metrics(user_data)

    return json.dumps(metrics)


@app.route('/dashboard/proba',methods=['POST'])
def proba():

    results=request.form

    results=dict(results)

    user_dat=results_parser(results)

    user_dat['value']=int(user_dat['price'])-int(user_dat['dereg_value'])

    proba=pred_prob_sell(user_dat)



    return json.dumps({'proba':int(np.round(proba[0][1]*100))})



if __name__ == '__main__':

    app.run(debug=True)

