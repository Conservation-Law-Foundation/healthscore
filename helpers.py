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

def add_data_one(call,db,obj):
    #make api call and change to pd
    data = requests.get(call)
    df = pd.DataFrame.from_dict(data.json())
    df.columns = df.iloc[0]
    df.drop(df.index[0], inplace=True)
    #add data to object
    for m in db.metrics:
        #create new metric to put in Place
        new_Metric = Metric(m.name, m.code)
        setattr(new_Metric, 'E', df[m.code_E].iloc[0])  
        setattr(new_Metric, 'M', df[m.code_M].iloc[0]) 
        #put the metric in Place
        setattr(obj, m.name, new_Metric)

#def add_data_csv()

def add_data_all_tracts(db,plcs):
    for i in range(1, len(plcs)):
        ct = db.call_tract
        add_data_one(ct[i-1], db, plcs[i])

def add_data(db,plcs):
    add_data_all_tracts(db,plcs)
    add_data_one(db.call_state, db, plcs[0])

def add_data_csv(plcs,db):
    for p in plcs:
        for m in db.metrics:
        #create new metric to put in Place
            new_Metric = Metric(m, '')
            setattr(new_Metric, 'E', db.data.loc[db.data['TRACT2KX'] == float(p.code), 'e(0)'].iloc[0])
            setattr(new_Metric, 'M', db.data.loc[db.data['TRACT2KX'] == float(p.code), 'se(e(0))'].iloc[0])  
            #put the metric in Place
            setattr(p, m, new_Metric)

def compile_data(plcs):
    all_attr = [a for a in dir(plcs[1]) if not a.startswith('__') and not a.startswith('name') and not a.startswith('code')]
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
        #data_row = np.zeros(shape=(1, num_places*2))
        #filled = 0
        for p in plcs:
            m = p.__dict__[a]
            # data_row[filled] = m.E
            # print(data_row)
            # filled += 1
            # data_row[filled] = m.M
            # filled += 1
            data_row.append(float(m.E))
            data_row.append(float(m.M))
        #filled = 0
        data[i] = data_row

    data = np.array(data, dtype=object)
    df = pd.DataFrame(data.T, index=col, columns = all_attr)
    return df