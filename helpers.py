#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 16:00:29 2021

@author: emilyfang
"""
import requests
import pandas as pd
import numpy as np
from classes import *
import time

#HELPER FUNCTIONS - DO NOT EDIT

#####
#ACS#
#####
def create_acs_calls(db,plcs):
    #make the calls
    count = 1
    call_bases = []   
    call = db.call_base    
    for m in db.metrics:
        call = call + m.code_E + ',' + m.code_M + ','
        count += 2
        if count > 48:
            call_bases.append(call)
            call = db.call_base
            count = 1   
    call_bases.append(call) 
    #make calls for each place 
    
    #state
    state_var = plcs[0]
    for c in call_bases:
        old = db.call_state
        new = c[:-1] + db.call_end_state
        old.append(new)
        db.call_state = old

    #tracts
    for i in range(1, len(plcs)):
        call_list = []
        for c in call_bases:
            new = c[:-1] + db.call_end_tract.replace("XXXXXX", plcs[i].code)
            call_list.append(new)
        db.call_tract.append(call_list)

def add_acs_data_one(call,db,obj,base_df):
    data = requests.get(call)
    #print(call)
    #print(data)
    df = pd.DataFrame.from_dict(data.json())
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    #print(df['B19013_001M'])
    #add data to dataframe
    for m in db.metrics:
        try:
            base_df.loc[m.name, (obj.code, 'EST')] = float(df[m.code_E].iloc[0])
            base_df.loc[m.name, (obj.code, 'MOE')] = float(df[m.code_M].iloc[0])
            #print(base_df)
            base_df.loc[m.name, 'Source'] = 'ACS'
        except KeyError:
            continue

def add_acs_data_all(db,plcs,base_df):
    #all tracts
    for i in range(1, len(plcs)):
        spec_tracts = db.call_tract[i-1]
        for c in spec_tracts:
            add_acs_data_one(c, db, plcs[i], base_df)
    #state
    for c in db.call_state:
        add_acs_data_one(c, db, plcs[0], base_df) 


#RWJF
def add_rwjf_data(plcs,db,base_df):
    for p in plcs:
        for m in db.metrics:
            base_df.loc[m, (p.code, 'EST')] = db.data.loc[db.data['Tract ID'] == float(p.full_code), 'e(0)'].iloc[0]
            base_df.loc[m, (p.code, 'SE')] = db.data.loc[db.data['Tract ID'] == float(p.full_code), 'se(e(0))'].iloc[0] #standard error, not margin of error
            base_df.loc[m, 'Source'] = 'RWJF'


#CDC PLACES
def add_places_data(db, plcs, client, base_df):
    db.update_metrics()
    for m in db.metrics:
        base_df.loc[m.name, 'Source'] = 'PLACES'
        for p in plcs:
            results = client.get("cwsq-ngmh", year="2018", locationid=p.full_code, measureid=m.code, limit=2000)
            df = pd.DataFrame.from_records(results)
            est = float(df['data_value'])
            pop = float(df['totalpopulation'])
            me = (abs((est - float(df['low_confidence_limit']))) + abs((est - float(df['high_confidence_limit'])))) / 2
            base_df.loc[m.name, (p.code, 'EST')] = est
            base_df.loc[m.name, (p.code, 'MOE')] = me
            base_df.loc['PLACES population', (p.code, 'EST')] = pop
            base_df.loc['PLACES population', (p.code, 'MOE')] = 0

#EJ SCREEN
def create_EJ_calls(db,tracts):
    for i in range(len(tracts)):
        old = db.call_base
        new = db.call_base.replace("XXXXXXXXXXX", tracts[i].full_code)
        db.call_tract.append(new)

def add_ej_data_one(call, db, obj, state, base_df):
    response = requests.get(call)
    data = response.json()
    #print(data)
    for m in db.metrics:
        base_df.loc[m.name, (obj.code, 'EST')] = float(data[m.code_E])
        base_df.at[m.name, (state.code, 'EST')] = float(data[m.code_S])
        base_df.at[m.name, (obj.code, 'PERC')] = float(data[m.code_P])
        base_df.loc[m.name, 'Source'] = 'EJ'

def add_ej_data_all(db, plcs, base_df):
    for i in range(1, len(plcs)):
        ct = db.call_tract
        add_ej_data_one(ct[i-1], db, plcs[i], plcs[0], base_df)    

#LATCH
def add_latch_data(plcs,db,base_df):
    for p in plcs:
        for m in db.metrics_dict.keys():
            base_df.loc[m, (p.code, 'EST')] = db.data.loc[db.data['geocode'] == float(p.full_code), db.metrics_dict[m]].iloc[0]
            base_df.loc[m, 'Source'] = 'LATCH'


# def add_all_EJ_data(db,plcs):
#     for i in range(1, len(plcs)):
#         ct = db.call_tract
#         add_EJ_data_one(ct[i-1], db, plcs[i])

def compile_data(plcs):
    all_attr = [a for a in dir(plcs[1]) if not a.startswith('__') and not a.startswith('name') and not a.startswith('code') and not a.startswith('full_code')]
    num_metrics = len(all_attr)
    num_places = len(plcs)
    level1 = []
    level2 = []
    for p in plcs:
        level1.append(p.name)
        level1.append(p.name)
        level2.append('E')
        level2.append('M')  
    col = pd.MultiIndex.from_arrays([level1, level2])
    data = np.zeros(shape=(num_metrics, num_places*2))
    for i in range(0,len(all_attr)):
        a = all_attr[i]
        data_row = []
        for p in plcs:
            m = p.__dict__[a]
            data_row.append(float(m.E))
            data_row.append(float(m.M))
        data[i] = data_row

    data = np.array(data, dtype=object)
    df = pd.DataFrame(data.T, index=col, columns = all_attr)
    return df

def add_schools(state, district, base):
    #MA
    if state == '25':
        #school performance
        edu = pd.read_excel('accountability-data-2019.xlsx', sheet_name='Table 3 - Schools', usecols='D,L', header=0)
        edu.columns = edu.iloc[0]
        edu = edu[1:]
        base.loc['School Performance - Overall', ('All Tracts', 'PERC')] = edu.loc[edu['District Name'] == district]['2019 Accountability Percentile'].mean(axis=0)
        
        #disadvantaged
        edu_dis = pd.read_excel('subgroup-percentile-2019.xlsx', None)
        percentiles = []
        
        nonhs = edu_dis['NonHS data']
        hs = edu_dis['HS data']
        k12 = edu_dis['MSHS K-12 data']
        
        sheets = [nonhs, hs, k12]
        
        for i in sheets:
            
            num_cols = len(i.columns)
            in_district = i['District'].isin([district])
            school_list = []
            
            if any(in_district):   
                schools = i[in_district]['School name'].unique()
                district_data = i[in_district]
                for s in schools:
                    school_data = district_data.loc[district_data['School name'] == s]
                    econ_dis = school_data.loc[school_data['Group'] == "Econ. Disadvantaged"]
                    high_needs = school_data.loc[school_data['Group'] == "High needs"]
                    if econ_dis.empty and high_needs.empty:
                        pass
                    elif econ_dis.empty:
                        percentiles.append(high_needs.iat[0, num_cols-1])
                    else:
                        percentiles.append(econ_dis.iat[0, num_cols-1])
            else:
                pass
        
        base.at['School Performance - Econ. Disadvantaged', ('All Tracts', 'PERC')] = np.sum(percentiles) / len(percentiles)

    #CT
    if state == '09':
        edu = pd.read_excel('nextgenacct.xls', header=2)
        base.at['School Performance - Overall', 'All Tracts'] = edu.loc[edu['RptngDistrictName'] == district]['OutcomeRatePct'].mean(axis=0)
    
    #RI
    if state == '44':
        edu = pd.read_excel('Accountability_201819.xlsx', sheet_name='School Indicator Data')
        #calculate 67th percentile
        all_districts = edu['District'].unique()
        all_districts = all_districts[1:]
        all_scores = []
        for d in all_districts:
            score = edu.loc[(edu['District'] == d) & (edu['Group'] == 'All Students')]['2019 Star Rating'].mean(axis=0)
            all_scores.append(score)
        cutoff = np.nanpercentile(all_scores, 67)    
        base.at['School Performance - Overall', 'All Tracts'] = edu.loc[edu['District'] == district & edu['Group'] == 'All Students']['2019 Star Rating'].mean(axis=0)

##############
#CALCULATIONS#
##############

def row_avg(metric, base, tracts):
    sums = 0
    for t in tracts:
        sums += base.loc[metric, (t, 'EST')]
    base.loc[metric, ('All Tracts', 'EST')] = sums/len(tracts)

def perc_avg(metric, base, tracts):
    sums = 0
    for t in tracts:
        sums += base.loc[metric, (t, 'PERC')]
    base.loc[metric, ('All Tracts', 'PERC')] = sums/len(tracts)

def add_est(calc, metric_list, base, col):
    sums = 0
    for m in metric_list:
        sums += base.loc[m, (col, 'EST')]
    base.loc[calc, (col, 'EST')] = sums
    base.loc[calc, 'Source'] = base.loc[metric_list[0], 'Source'].to_numpy()

def subtract_est(calc, first, second, base, col):
    assign = (col, 'EST')
    base.loc[calc, assign] = base.loc[first, assign] - base.loc[second, assign]
    base.loc[calc, 'Source'] = base.loc[first, 'Source'].to_numpy()
    
def divide_est(calc, num, den, base, col):
    base.loc[calc, (col, 'EST')] = base.loc[num, (col, 'EST')] / base.loc[den, (col, 'EST')] * 100
    base.loc[calc, 'Source'] = base.loc[num, 'Source'].to_numpy()

def add_moe(calc, metric_list, base, col):    
    if base.loc[metric_list[0], 'Source'].to_numpy() == 'PLACES':
        z = 1.96
    else:
        z = 1.645
    sums = 0
    for m in metric_list:
        sums += ((1/z)*base.loc[m, (col, 'MOE')])**2
    sums = z * sums**(1/2)
    base.loc[calc, (col, 'MOE')] = sums
    base.loc[calc, 'Source'] = base.loc[metric_list[0], 'Source'].to_numpy()

def divide_moe(calc, num, den, frac, base, col):    
    MOE_num = base.loc[num, (col, 'MOE')]
    MOE_den = base.loc[den, (col, 'MOE')]
    R = base.loc[frac, (col, 'EST')] / 100
    X_den = base.loc[den, (col, 'EST')]
    MOE_calc = ((MOE_num**2 - (R**2).to_numpy() * MOE_den**2)**(1/2)).to_numpy() / X_den.to_numpy() * 100
    if isinstance(MOE_calc, (int, float)):
        base.loc[calc, (col, 'MOE')] = MOE_calc
    else: 
        MOE_calc = ((MOE_num**2 + (R**2).to_numpy() * MOE_den**2)**(1/2)).to_numpy() / X_den.to_numpy() * 100
        base.loc[calc, (col, 'MOE')] = MOE_calc
    base.loc[calc, 'Source'] = base.loc[num, 'Source'].to_numpy()
    
def divide_moe_all_only(calc, num, den, frac, base):    
    MOE_num = base.loc[num, ('All Tracts', 'MOE')]
    MOE_den = base.loc[den, ('All Tracts', 'MOE')]
    R = base.loc[frac, ('All Tracts', 'EST')] / 100
    X_den = base.loc[den, ('All Tracts', 'EST')]
    MOE_calc = ((MOE_num**2 - (R**2) * MOE_den**2)**(1/2)) / X_den * 100
    if isinstance(MOE_calc, (int, float)):
        base.loc[calc, ('All Tracts', 'MOE')] = MOE_calc
    else: 
        MOE_calc = ((MOE_num**2 + (R**2) * MOE_den**2)**(1/2)) / X_den * 100
        base.loc[calc, ('All Tracts', 'MOE')] = MOE_calc
    base.loc[calc, 'Source'] = base.loc[num, 'Source'].to_numpy()

def agg_moe(tracts, base):
    for index, row in base.iterrows():
        est = base.loc[index, (tracts, 'EST')]
        moe = base.loc[index, (tracts, 'MOE')]
        est.reset_index(drop=True, inplace=True)
        moe.reset_index(drop=True, inplace=True)
        #find count of zeros
        count0 = est.isin([0]).sum()
        if count0 <= 1:
            base.loc[index, ('All Tracts', 'MOE')] = (((moe)**2).sum())**(1/2)
        else:
            #find location of zeros
            which0 = est.isin([0])
            #which0.reset_index(drop=True, inplace=True)
            
            # sum squared non-zeros
            non0s = ((moe[~which0])**2).sum()
            with0s = non0s + ((moe[which0])**2).max()
            moe_all = with0s**(1/2)
            base.loc[index, ('All Tracts', 'MOE')] = moe_all    

def agg_moe_row(tracts, metric_list, base):
    for m in metric_list:
        est = base.loc[m, (tracts, 'EST')]
        moe = base.loc[m, (tracts, 'MOE')]
        est.reset_index(drop=True, inplace=True)
        moe.reset_index(drop=True, inplace=True)
        #find count of zeros
        count0 = est.isin([0]).sum()
        if count0 <= 1:
            base.loc[m, ('All Tracts', 'MOE')] = (((moe)**2).sum())**(1/2)
        else:
            #find location of zeros
            which0 = est.isin([0])
            #which0.reset_index(drop=True, inplace=True)
            
            # sum squared non-zeros
            non0s = ((moe[~which0])**2).sum()
            with0s = non0s + ((moe[which0])**2).max()
            moe_all = with0s**(1/2)
            base.loc[m, ('All Tracts', 'MOE')] = moe_all    

def rollup_percent_calc(metric, total_pop, base, tracts, state, col):
    temp = 'Total ' + metric
    sums = 0
    for t in tracts:
        base.loc[temp, (t, 'EST')] = (base.loc[metric, (t, 'EST')]) * base.loc[total_pop, (t, 'EST')] / 100
        base.loc[temp, (t, 'MOE')] = (base.loc[metric, (t, 'MOE')]) * base.loc[total_pop, (t, 'MOE')] / 100
        sums += base.loc[temp, (t, 'EST')]
    base.loc[temp, ('All Tracts', 'EST')] = sums
    base.loc[metric, ('All Tracts', 'EST')] = sums / base.loc[total_pop, ('All Tracts', 'EST')] * 100
    #handle state
    base.loc[temp, (state, 'EST')] = (base.loc[metric, (state, 'EST')]) * base.loc[total_pop, (state, 'EST')] / 100
    base.loc[temp, (state, 'MOE')] = (base.loc[metric, (state, 'MOE')]) * base.loc[total_pop, (state, 'MOE')] / 100
    #aggregate
    agg_moe_row(tracts, [temp], base)
    #divide_moe_all_only(metric, temp, total_pop, metric, base)
    #agg_moe(tracts, base[[metric, total_pop, temp], :])
    
def rollup_num_calc(metric, total_pop, base, tracts, state, col):
    temp = 'Total ' + metric
    sums = 0
    for t in tracts:
        base.loc[temp, (t, 'EST')] = (base.loc[metric, (t, 'EST')]) * base.loc[total_pop, (t, 'EST')]
        base.loc[temp, (t, 'MOE')] = (base.loc[metric, (t, 'MOE')]) * base.loc[total_pop, (t, 'MOE')]
        sums += base.loc[temp, (t, 'EST')]
    base.loc[temp, ('All Tracts', 'EST')] = sums
    base.loc[metric, ('All Tracts', 'EST')] = sums / base.loc[total_pop, ('All Tracts', 'EST')]
    #handle state
    base.loc[temp, (state, 'EST')] = (base.loc[metric, (state, 'EST')]) * base.loc[total_pop, (state, 'EST')]
    base.loc[temp, (state, 'MOE')] = (base.loc[metric, (state, 'MOE')]) * base.loc[total_pop, (state, 'MOE')]
    #aggregate
    agg_moe_row(tracts, [temp], base)
    #divide_moe(metric, temp, total_pop, metric, base, col)
    #agg_moe(tracts, base[[metric, total_pop, temp], :])
       

def divide_rows(calc, num, den, frac, base, col):
    divide_est(calc, num, den, base, col)  
    divide_moe(calc, num, den, frac, base, col)                                       
                                           