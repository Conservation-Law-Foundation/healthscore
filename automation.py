#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
@author: emilyfang
"""
import requests
import pandas as pd
import numpy as np
from helpers import *
from classes import *
import calcs
from sodapy import Socrata

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.options.display.float_format = '{:.1f}'.format #set number of decimal points

# #SPECIFIC TRACT INPUT
# #Holbrook: tracts are 421100, 420302, 421200
# state = str(input("State: "))
# county = str(input("County: "))
# tracts = []
# stop = False
# while stop == False:
#     t = str(input("Tract: "))
#     if t == 'STOP':
#         stop = True;
#     else:
#         tracts.append(t)
# project = str(input("Project Name: " ))

#HARD CODED TRACT INPUT
state = str(25)
county = str(0)+str(21)
#tract = str(421100)
tracts = ['421100','420302','421200']
project = 'Holbrook'

    
#TRACT & STATE OBJECTS
State = Place('MA', state)
places = [State]
Tracts = []
for t in tracts:
    T = Place(project, t)
    setattr(T, 'full_code', state+county+t)
    places.append(T)
    Tracts.append(T)

#DATA PULLS
import databases

#ALL ACS
for db in databases.dbs:
    db.update_metrics()
    create_acs_calls(db, places)
    add_data(db, places)

# #CDC LIFE EXPECTANCY
#cdc_data = databases.cdc_data
CDC = databases.CDC
add_data_csv(Tracts, CDC)

# for m in CDC.metrics:
#     new_Metric = Metric(m, '')
#     setattr(new_Metric, 'E', 0)
#     setattr(new_Metric, 'M', 0)
#     setattr(State, m, new_Metric)
    
#CDC PLACES
PLACES = databases.PLACES
client = databases.client
PLACES.update_metrics()
#put TRACTS in instead of PLACES

    
add_PLACES_data(PLACES, Tracts, client)

for m in PLACES.metrics:
    new_Metric = Metric(m, '')
    setattr(new_Metric, 'E', 0)
    setattr(new_Metric, 'M', 0)
    setattr(State, m.name, new_Metric)

#MA DATA FROM OTHER SOURCES
MA_data = pd.read_excel('MA_DATA.xlsx', index_col=0)
for i in MA_data.index:
    new_Metric = Metric(i, '')
    setattr(new_Metric, 'E', MA_data.loc[i]['E'])
    setattr(new_Metric, 'M', MA_data.loc[i]['M'])
    setattr(State, i, new_Metric)

#COMPILATION
df_proj = compile_data(places)

calcs.calculations(df_proj)
calcs.touchups_and_export(df_proj)
