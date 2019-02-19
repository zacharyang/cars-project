from flask import Flask, render_template, url_for, request
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



connection=psycopg2.connect("host=localhost dbname=sgcarmart user=postgres password=postgres")
engine = create_engine('postgresql://localhost:5432/sgcarmart')


supported_model_query='''

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

supported_models=engine.execute(supported_model_query).fetchall()
supported_models_df=pd.DataFrame(supported_models,columns=['model','brand'])

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
            ORDER BY model ASC


            '''

all_models=engine.execute(all_model_query).fetchall()
all_models_df=pd.DataFrame(all_models,columns=['model','brand'])

res = defaultdict(list)
for v, k in all_models: 
    res[k].append(v)
model_list=[{'text':k, 'children':v} for k,v in res.items()]
i=0
for brand in model_list:
    models=[]
    for model in brand['children']:
        d={}
        d['id']=i
        d['text']=model
        i+=1
        models.append(d)
    brand['children']=models


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

def parf_calculator(arf,veh_age_mths):
    veh_age=np.floor(veh_age_mths/12)
    if veh_age<5:
        return 0.75*(arf)
    if 5<=veh_age<10:
        return (0.75-0.05*(veh_age-4))*(arf)
    if x.veh_age>=10:
        return 0

def arf_calculator(x):
    arf=20000
    if x<=20000:
        return arf
    if 20000<x<=50000:
        return arf + 1.4*(x-arf)
    if x>50000:
        return arf + 1.4*(x-arf) +1.8*(x-50000)



app = Flask(__name__)

@app.route('/')
def home():
    return render_template('home.html',models=model_list)

@app.route('/autofill',methods=['POST'])
def autofill():

    autofill=request.get_json()
    model_id=int(autofill['model'])
    model=supported_models_df.loc[model_id]['model']
    brand=supported_models_df.loc[model_id]['brand']
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
    price_predictor=joblib.load('PRICE.pkl')


    model_id=int(results['model'])
    model=all_models_df.loc[model_id]['model']
    models=all_models_df['model'][1:]
    model_v=[1 if m==model else 0 for m in models]


    omv=int(results['omv'])
    mileage=results['mileage']

    reg_month=str(int(results['reg_month'])+1)
    reg_year=results['reg_year']
    reg_my=reg_month+'-'+str(reg_year)
    reg_my_dt=datetime.datetime.strptime(reg_my,'%m-%Y')
    
    veh_age_mths=pd.Timestamp(datetime.datetime.now()).to_period('M')-pd.Timestamp(reg_my_dt).to_period('M')
    if type(veh_age_mths)!= int:
        veh_age_mths=veh_age_mths.n
    arf=arf_calculator(omv)
    coe=int(results['coe'])

    if 'coe_extend' in results.keys():
        n_years_renewed=int(results['coe_extend'])
        coe_expiry_date=reg_my_dt+ datetime.timedelta(days=365.25*(n_years_renewed+10))
        month_to_coe_expiry=pd.Timestamp(coe_expiry_date).to_period('M')-pd.Timestamp(datetime.datetime.now()).to_period('M')
        if type(month_to_coe_expiry)!=int:
            month_to_coe_expiry=month_to_coe_expiry.n
        
        coe_rebate = coe*month_to_coe_expiry/ (n_years_renewed*12)
        parf_rebate=0
    else:
        coe_expiry_date=reg_my_dt + datetime.timedelta(days=365.25*10)
        month_to_coe_expiry=pd.Timestamp(coe_expiry_date).to_period('M')-pd.Timestamp(datetime.datetime.now()).to_period('M')
        if type(month_to_coe_expiry)!=int:
            month_to_coe_expiry=month_to_coe_expiry.n
        parf_rebate=parf_calculator(arf,veh_age_mths)
        coe_rebate= coe * month_to_coe_expiry/120

  

    dereg_value = coe_rebate + parf_rebate

   

    engine_cap=results['engine_cap']
    power=results['power']
    curb_weight=results['curb_weight']

    vals=np.array([engine_cap, power, curb_weight, omv, mileage, veh_age_mths, month_to_coe_expiry]+model_v).reshape(1,-1)

    if vals.shape[1]!=price_predictor.n_features_:
        raise Exception('Incorrect number of features. Try re-training the model')

    value=int(np.round(price_predictor.predict(vals)[0]))
    print('coe_rebate:{}, parf_rebate:{}, prediction:{},veh_age_mths:{}'.format(coe_rebate,parf_rebate,value,veh_age_mths))

    price = value + int(dereg_value)
    price = '{:,}'.format(price)
    return json.dumps({'price':price})


if __name__ == '__main__':
    app.run(debug=True)

