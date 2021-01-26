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

#HELPER FUNCTIONS - DO NOT EDIT

#####
#ACS#
#####
def create_acs_calls(db,plcs):
    for m in db.metrics:
        db.call_base = db.call_base + m.code_E + ',' + m.code_M + ','
    for i in range(len(plcs)):
        if i == 0: #first one is always the state
            db.call_state = db.call_base[:-1] + db.call_end_state
        else:
            old = db.call_tract
            new = db.call_base[:-1] + db.call_end_tract.replace("XXXXXX", plcs[i].code)
            old.append(new)
            db.call_tract = old

def add_acs_data_one(call,db,obj,base_df):
    data = requests.get(call)
    df = pd.DataFrame.from_dict(data.json())
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    #add data to dataframe
    for m in db.metrics:
        base_df.at[m.name, obj.name] = float(df[m.code_E].iloc[0])

def add_acs_data_all(db,plcs,base_df):
    #all tracts
    for i in range(1, len(plcs)):
        ct = db.call_tract
        add_acs_data_one(ct[i-1], db, plcs[i], base_df)
    #state
    add_acs_data_one(db.call_state, db, plcs[0], base_df) 

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
            base_df.at[m, p.name] = db.data.loc[db.data['Tract ID'] == float(p.full_code), 'e(0)'].iloc[0]

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
            #me = est - float(df['low_confidence_limit'])
            base_df.at[m.name, p.name] = est
            base_df.at['PLACES population', p.name] = pop
    

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
    for m in db.metrics:
        base_df.at[m.name, obj.name] = float(data[m.code_E])
        base_df.at[m.name, state.name] = float(data[m.code_S])

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
        for m in db.metrics:
            base_df.at[m, p.name] = db.data.loc[db.data['geocode'] == float(p.full_code), 'est_vmiles'].iloc[0]


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


#CALCULATIONS
def rollup_percent_calc(metric, total_pop, base_df):
    end = base_df.columns.get_loc('All Tracts')
    start = base_df.columns[0]
    stop = base_df.columns[end-1]
    stop_extra = base_df.columns[end]
    temp = 'Total ' + metric
    base_df.loc[temp, start:stop] = (base_df.loc[metric, start:stop]) \
    * base_df.loc[total_pop, start:stop] / 100
    base_df.at[temp, 'All Tracts'] = base_df.loc[temp, start:stop].sum(axis=0)
    base_df.loc[metric, start:stop_extra] = (base_df.loc[temp, start:stop_extra]) \
    / base_df.loc[total_pop, start:stop_extra] * 100

def row_avg(metric, base_df):
    end = base_df.columns.get_loc('All Tracts')
    base_df.loc[metric, 'All Tracts'] = (base_df.loc[metric, base_df.columns[0]:base_df.columns[end-1]]).sum(axis=0) / end