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
    #print(data)
    df = pd.DataFrame.from_dict(data.json())
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    #add data to dataframe
    for m in db.metrics:
        try:
            base_df.at[m.name, obj.code] = float(df[m.code_E].iloc[0])
            base_df.at[m.name, 'MOE' + obj.code] = float(df[m.code_M].iloc[0])
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

# def add_data_one(call,db,obj):
#     #make api call and change to pd
#     data = requests.get(call)
#     df = pd.DataFrame.from_dict(data.json())
#     df.columns = df.iloc[0]
#     df.drop(df.index[0], inplace=True)
#     #add data to object
#     for m in db.metrics:
#         #create new metric to put in Place
#         new_Metric = Metric(m.name, m.code)
#         setattr(new_Metric, 'E', df[m.code_E].iloc[0])  
#         setattr(new_Metric, 'M', df[m.code_M].iloc[0]) 
#         #put the metric in Place
#         setattr(obj, m.name, new_Metric)

# def add_data_all_tracts(db,plcs):
#     for i in range(1, len(plcs)):
#         ct = db.call_tract
#         add_data_one(ct[i-1], db, plcs[i])

# def add_data(db,plcs):
#     add_data_all_tracts(db,plcs)
#     add_data_one(db.call_state, db, plcs[0])


#RWJF
def add_rwjf_data(plcs,db,base_df):
    for p in plcs:
        for m in db.metrics:
            base_df.at[m, p.code] = db.data.loc[db.data['Tract ID'] == float(p.full_code), 'e(0)'].iloc[0]
            base_df.at[m, 'MOE' + p.code] = db.data.loc[db.data['Tract ID'] == float(p.full_code), 'se(e(0))'].iloc[0] #standard error, not margin of error

# def add_data_csv(plcs,db):
#     for p in plcs:
#         for m in db.metrics:
#         #create new metric to put in Place
#             new_Metric = Metric(m, '')
#             setattr(new_Metric, 'E', db.data.loc[db.data['Tract ID'] == float(p.full_code), 'e(0)'].iloc[0])
#             setattr(new_Metric, 'M', db.data.loc[db.data['Tract ID'] == float(p.full_code), 'se(e(0))'].iloc[0])  
#             #put the metric in Place
#             setattr(p, m, new_Metric)

#CDC PLACES
def add_places_data(db, plcs, client, base_df):
    db.update_metrics()
    for m in db.metrics:
        for p in plcs:
            results = client.get("cwsq-ngmh", year="2018", locationid=p.full_code, measureid=m.code, limit=2000)
            df = pd.DataFrame.from_records(results)
            est = float(df['data_value'])
            pop = float(df['totalpopulation'])
            me = (abs((est - float(df['low_confidence_limit']))) + abs((est - float(df['high_confidence_limit'])))) / 2
            base_df.at[m.name, p.code] = est
            base_df.at[m.name, 'MOE' + p.code] = me
            base_df.at['PLACES population', p.code] = pop
    

# def add_PLACES_data(db, plcs, client):
#     db.update_metrics()
#     for m in db.metrics:
#         for p in plcs:
#             results = client.get("cwsq-ngmh", year="2018", locationid=p.full_code, measureid=m.code, limit=2000)
#             df = pd.DataFrame.from_records(results)
#             est = float(df['data_value'])
#             me = est - float(df['low_confidence_limit'])
#             new_Metric = Metric(m.name, m.code)
#             setattr(new_Metric, 'E', est)
#             setattr(new_Metric, 'M', me)
#             setattr(p, m.name, new_Metric)

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
        base_df.at[m.name, obj.code] = float(data[m.code_E])
        base_df.at[m.name, state.code] = float(data[m.code_S])
        base_df.at[m.name, 'P' + obj.code] = float(data[m.code_P])

def add_ej_data_all(db, plcs, base_df):
    for i in range(1, len(plcs)):
        ct = db.call_tract
        add_ej_data_one(ct[i-1], db, plcs[i], plcs[0], base_df)    

# def add_EJ_data_one(call,db,tracts,state):
#     #make api call and change to pd
#     data = requests.get(call)
#     for m in db.metrics:
#         new_Metric = Metric(m.name, m.code)
#         setattr(new_Metric, 'E', data[m.code_E])  
#         setattr(new_Metric, 'M', 0) 
#         setattr(t, m.name, new_Metric)
#         #for the state
#         new_Metric = Metric(m.name, m.code)
#         setattr(new_Metric, 'E', data[m.code_S])
#         setattr(new_Metric, 'M', 0)
#         setattr(state, m.name, new_Metric)

#LATCH
def add_latch_data(plcs,db,base_df):
    for p in plcs:
        for m in db.metrics_dict.keys():
            base_df.at[m, p.code] = db.data.loc[db.data['geocode'] == float(p.full_code), db.metrics_dict[m]].iloc[0]


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
        base.at['School Performance - Overall', 'All Tracts'] = edu.loc[edu['District Name'] == district]['2019 Accountability Percentile'].mean(axis=0)
        
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
        
        base.at['School Performance - Econ. Disadvantaged', 'All Tracts'] = np.sum(percentiles) / len(percentiles)

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

def rollup_percent_calc(metric, total_pop, base_df):
    end = base_df.columns.get_loc('All Tracts')
    start = base_df.columns[0]
    stop = base_df.columns[end-1]
    stop_extra = base_df.columns[end]
    temp = 'Total ' + metric
    base_df.loc[temp, start:stop:2] = (base_df.loc[metric, start:stop]) \
    * base_df.loc[total_pop, start:stop:2] / 100
    base_df.at[temp, 'All Tracts'] = base_df.loc[temp, start:stop:2].sum(axis=0)
    base_df.loc[metric, start:stop_extra:2] = (base_df.loc[temp, start:stop_extra:2]) \
    / base_df.loc[total_pop, start:stop_extra:2] * 100


def row_avg(metric, base_df, tracts):
    end = base_df.columns.get_loc('All Tracts')
    stop = base_df.columns[end-1] 
    start = base_df.columns[0]
    base_df.loc[metric, 'All Tracts'] = base_df.loc[metric, start:stop:2].sum(axis=0) / len(tracts)

def add_moe(metric_list, start_m, stop_m, skip, base):    
    sums = 0
    for m in metric_list:
        sums += ((1/1.645)*base.loc[m, start_m:stop_m:skip])**2
    sums = 1.645 * sums**(1/2)
    
    return sums

def divide_moe(num, den, frac, start_m, stop_m, skip, start_e, stop_e, base):
    start_m = base.columns[1]
    end = base.columns.get_loc('All Tracts')
    stop_m = base.columns[end-1]
    start_e = base.columns[0]
    stop_e = base.columns[end-2]
    MOE_num = base.loc[num, start_m:stop_m:2]
    MOE_den = base.loc[den, start_m:stop_m:2]
    R = base.loc[frac, start_e:stop_e:2]
    X_den = base.loc[den, start_e:stop_e:2]
    MOE_calc = ((MOE_num**2 - (R**2).to_numpy() * MOE_den**2)**(1/2)).to_numpy() / X_den.to_numpy()
    return MOE_calc

def agg_moe(base):
    start_m = base.columns[1]
    end = base.columns.get_loc('All Tracts')
    stop_m = base.columns[end-1]
    start_e = base.columns[0]
    stop_e = base.columns[end-2]
    for index, row in base.iterrows():
        est = base.loc[index, start_e:stop_e:2]
        moe = base.loc[index, start_m:stop_m:2]
        est.reset_index(drop=True, inplace=True)
        moe.reset_index(drop=True, inplace=True)
        #find count of zeros
        count0 = est.isin([0]).sum()
        if count0 <= 1:
            base.at[index, 'MOE All'] = (((moe)**2).sum())**(1/2)
        else:
            #find location of zeros
            which0 = est.isin([0])
            #which0.reset_index(drop=True, inplace=True)
            
            # sum squared non-zeros
            non0s = ((moe[~which0])**2).sum()
            with0s = non0s + ((moe[which0])**2).max()
            moe_all = with0s**(1/2)
            base.at[index, 'MOE All'] = moe_all
    return base                                                     
                                           