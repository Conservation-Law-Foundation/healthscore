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

# PANDAS SETTINGS - DO NOT EDIT
pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.options.display.float_format = '{:.2f}'.format #set number of decimal points

#SPECIFIC TRACT INPUT
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
col = []
for t in tracts:
    T = Place(project, t)
    setattr(T, 'full_code', state+county+t)
    places.append(T)
    Tracts.append(T)
    col.append(T.name)


#MAKE DATAFRAMES
temp = ['All Tracts', State.name]
col.extend(temp)
ind = []

import databases
db = databases.ACS_B
for m in db.metrics_dict.keys(): #add all pulled metrics to index
    ind.append(m)
base = pd.DataFrame(index=ind, columns=col)
#base = df.append(pd.Series(name='idk', dtype='float64'))

############
#DATA PULLS#
############

#ALL ACS
for db in databases.dbs:
    db.update_metrics()
    create_acs_calls(db, places)
    add_acs_data_all(db, places, base)

# RWJF LIFE EXPECTANCY
CDC = databases.CDC
add_rwjf_data(Tracts, CDC, base)

#CDC PLACES
PLACES = databases.PLACES
client = databases.client
PLACES.update_metrics()  
add_places_data(PLACES, Tracts, client, base)

# #EJ SCREEN
EJ = databases.EJ
EJ.update_EJ_metrics()
create_EJ_calls(EJ, Tracts)
add_ej_data_all(EJ, places, base)

#LATCH
LATCH = databases.LATCH
add_latch_data(Tracts, LATCH, base)

# #MA DATA FROM OTHER SOURCES
MA_data = pd.read_excel('MA_DATA.xlsx', index_col=0)
for i in MA_data.index:
    base.at[i, State.name] = MA_data.loc[i]['E']

##############
#CALCULATIONS#
##############

base['All Tracts'] = base.iloc[:, 0:len(tracts)].sum(axis=1)

#remove incorrect values manually
base.at['Life Expectancy', 'All Tracts'] = 'NaN'
base.at['Median Household Income', 'All Tracts'] = 'NaN'
base.at['Average weekday vehicle miles traveled per household', 'All Tracts'] = 'NaN'

#PLACES calculations

for m in PLACES.metrics_dict.keys():
    rollup_percent_calc(m, 'PLACES population', base)

#EJ Screen calculations
for m in EJ.metrics_dict.keys():
    row_avg(m, base)

#Transit Use
rollup_percent_calc('% Public Transit', 'Workers >16', base)
rollup_percent_calc('% Walked', 'Workers >16', base)
rollup_percent_calc('% Bicycle', 'Workers >16', base)

#Poverty Rate
base.loc['Poverty Rate (%)', :] = (base.loc['Below Poverty Level', :]) \
    / base.loc['Total with Poverty Data', :] * 100

#Unemployment Rate
base.loc['Unemployment Rate (%)', :] = (base.loc['Total Unemployed', :]) \
    / base.loc['In Labor Force', :] * 100

#Educational Attainment
base.loc['Associates, Bachelors degree, or higher (%)', :] = ((base.loc['> 25 with Associates', :]) + (base.loc['> 25 with Bachelors or higher', :])) \
    / base.loc['Total with Education Data >25', :] * 100

#Limited English-speaking Households
base.loc['Limited English-speaking Households (%)', :] = (base.loc['Limited English-speaking', :]) \
    / base.loc['Total with Language Data', :] * 100

#Population of Color
base.loc['Population of Color (%)', :] = (base.loc['Total with Race Data', :] - base.loc['Total White Alone', :]) \
    / base.loc['Total with Race Data', :] * 100

#Cost-Burdened Renters
base.loc['Cost-Burdened Renters (%)', :] = (base.loc['Rent 30.0-34.9%', :] + base.loc['Rent 35.0-39.9%', :] \
    + base.loc['Rent 40.0-49.9%', :] + base.loc['Rent >50.0%', :]) \
    / base.loc['Total with Rent Data', :] * 100
  
#Low-Income <5
base.loc['Population of Low-Income Children <5 (%)', :] = (base.loc['Low-Income <5', :]) \
    / base.loc['Total with Poverty Data', :] * 100

#Low-Income >65
base.loc['Population of Low-Income Seniors >65 (%)', :] = (base.loc['Low-Income >65', :]) \
    / base.loc['Total with Poverty Data', :] * 100
    
#Car Ownership
base.loc['Average Number of Cars Per Household', :] = (base.loc['No vehicles', :] * 0 \
    + base.loc['1 vehicle', :] * 1 \
    + base.loc['2 vehicles', :] * 2 \
    + base.loc['>3 vehicles', :] * 3) / (base.loc['Occupied Housing Units', :])

base['Ratio'] = base['All Tracts'] / base[State.name]
    
###########
#TOUCH-UPS#
###########

# #remove rows
# remove_list = ['Total with Race Data', 'Total White Alone', \
#                 'Total with Rent Data', 'Rent 30.0-34.9%', 'Rent 35.0-39.9%', 'Rent 35.0-39.9%', 'Rent 40.0-49.9%', 'Rent >50.0%']
# base.drop(remove_list, axis=0, inplace=True)



#reorder rows
row_order = ['Life Expectancy', \
             'Cancer (excluding skin cancer) among adults >= 18', \
             'Current asthma among adults >= 18', \
             'COPD among adults >= 18', \
             'Coronary heart disease among adults >= 18', \
             'Diabetes among adults >= 18', \
             'Stroke among adults >= 18', \
             'Mental health not good for >= 14 days among adults >= 18', \
             'PM 2.5 (ug/m3)', \
             'NATA Diesel PM (ug/m3)', \
             'NATA Air Toxics Cancer Risk (risk per MM)', \
             'NATA Respiratory Hazard Index', \
             '% Public Transit', \
             '% Walked', \
             '% Bicycle', \
             'Average weekday vehicle miles traveled per household', \
             'Median Household Income', \
             'Poverty Rate (%)', \
             'Unemployment Rate (%)', \
             'Associates, Bachelors degree, or higher (%)', \
             'Limited English-speaking Households (%)', \
             'Population of Color (%)', \
             'Cost-Burdened Renters (%)', \
             'Population of Low-Income Children <5 (%)', \
             'Population of Low-Income Seniors >65 (%)', \
             'Average Number of Cars Per Household']
base = base.reindex(row_order)




print(base)

# for i in MA_data.index:
#     new_Metric = Metric(i, '')
#     setattr(new_Metric, 'E', MA_data.loc[i]['E'])
#     setattr(new_Metric, 'M', MA_data.loc[i]['M'])
#     setattr(State, i, new_Metric)

# #COMPILATION
# df_proj = compile_data(places)

# calcs.calculations(df_proj)
# calcs.touchups_and_export(df_proj)

#FINAL METRIC ORDER
    # df_ORHD = df[['Life Expectancy', \
    #               'Cancer (excluding skin cancer) among adults >= 18', \
    #               'Current asthma among adults >= 18', \
    #               'COPD among adults >= 18', \
    #               'Coronary heart disease among adults >= 18', \
    #               'Diabetes among adults >= 18', \
    #               'Stroke among adults >= 18', \
    #               'Mental health not good for >= 14 days among adults >= 18', \
    #               'PM 2.5 (ug/m3)']]
    # #df_ORHD = df[[]]
    # df_TAU = df[['% Public Transit', '% Walked', '% Bicycle']]
    # df_OARE = df[['Median Household Income', \
    #           'Poverty Rate', \
    #           'Unemployment Rate', \
    #           '% with Associates/Bachelors Degree or higher', \
    #           'Population of Color (%)', '% Limited English Households', \
    #           '% Neighborhood Renters who are Cost-Burdened']]
