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
import statistics

# # PANDAS SETTINGS - DO NOT EDIT
# pd.set_option('display.max_columns', None)
# pd.set_option('expand_frame_repr', False)
# pd.options.display.float_format = '{:.2f}'.format #set number of decimal points

# #SPECIFIC TRACT INPUT
# # Holbrook: tracts are 421100, 420302, 421200
# state = str(input("State: "))
# county = str(input("County: "))
# primary = str(input('Primary Tract: '))
# tracts = [primary]
# stop = False
# while stop == False:
#     t = str(input("Tract: "))
#     if t == 'STOP':
#         stop = True;
#     else:
#         tracts.append(t)
# block = str(input('Block: '))
# project = str(input("Project Name: " ))
# district = str(input("District: " ))

#HARD CODED TRACT INPUT - HOLBROOK
state = str(25)
county = str(0)+str(21)
#tract = str(421100)
primary = '421100'
tracts = ['421100','420302','421200']
project = 'Holbrook code reopened'
block = str(4)
district = 'Holbrook'

# #HARD CODE HAMILTON
# state = str(25)
# county = str(0)+ str(0) + str(9)
# #tract = str(421100)
# primary = '215102'
# tracts = ['215102', '215101', '216100']
# block = str(4)
# project = 'Hamilton'
# district = 'Hamilton-Wenham'

#TRACT & STATE OBJECTS
State = Place('', state)
places = [State]
Tracts = []
col = []
for t in tracts:
    T = Place(project, t)
    setattr(T, 'full_code', state+county+t)
    places.append(T)
    Tracts.append(T)
    col.append(T.code)
    col.append('MOE' + T.code)
    #col.append('P' + T.code)


#MAKE DATAFRAMES
temp = ['All Tracts', State.code]
col.extend(temp)
ind = []

import databases
ACS_B = databases.ACS_B
for m in ACS_B.metrics_dict.keys(): #add all pulled metrics to index
    ind.append(m)
base = pd.DataFrame(index=ind, columns=col, dtype='object')

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

#EPA SMART LOCATION
epa = pd.read_csv('MA_tracts_d4c.csv')
geoid = int(state + county + primary + block)
base.at['Transit Frequency', 'All Tracts'] =  epa.loc[epa['GEOID10'] == geoid]['D4c'].values[0]

#LATCH
LATCH = databases.LATCH
add_latch_data(Tracts, LATCH, base)
#LATCH state data processing
urb = base.at['Urban Group', base.columns[0]]
ser = LATCH.data['geocode'].astype(str)
bools = ser.str.startswith(state)
inds = bools[bools].index
state_data = LATCH.data.loc[inds]
state_data = state_data.astype({'urban_group': 'Int64'})
#base.at['Average weekday vehicle miles traveled per household', State.code] = state_data.loc[state_data['urban_group'] == urb]['est_vmiles'].mean(axis=0)

state_miles = 0
for t in Tracts:
    urb = base.at['Urban Group', t.code]
    state_miles  += state_data.loc[state_data['urban_group'] == urb]['est_vmiles'].mean(axis=0)
base.at['Average weekday vehicle miles traveled per household', State.code] = state_miles / len(tracts)


# #MA DATA FROM OTHER SOURCES
MA_data = pd.read_excel('MA_DATA.xlsx', index_col=0)
for i in MA_data.index:
    base.at[i, State.code] = MA_data.loc[i]['E']
    base.at[i, 'MOE' + State.code] = MA_data.loc[i]['M']

#SCHOOL DATA
add_schools(state, district, base)

 
##############
#CALCULATIONS#
##############

for index, row in base.iterrows():
    if pd.isna(row['All Tracts']):
        sums = 0
        for t in tracts:
            sums += row[t]
        base.at[index, 'All Tracts'] = sums

start_m = base.columns[1]
end = base.columns.get_loc('All Tracts')
stop_m = base.columns[end-1]
start_e = base.columns[0]
stop_e = base.columns[end-2]

#remove incorrect values manually
#base.at['Life Expectancy', 'All Tracts'] = 'NaN'
#base.at['Median Household Income', 'All Tracts'] = 'NaN'
base.at['Average weekday vehicle miles traveled per household', 'All Tracts'] = 'NaN'

#Life Expectancy
rollup_percent_calc('Life Expectancy', 'Total Population', base)

#PLACES calculations
for m in PLACES.metrics_dict.keys():
    rollup_percent_calc(m, 'PLACES population', base)

#EJ Screen calculations
for m in EJ.metrics_dict.keys():
    row_avg(m, base, tracts)

#Transit Use
rollup_percent_calc('% Public Transit', 'Workers >16', base)
rollup_percent_calc('% Walked', 'Workers >16', base)
rollup_percent_calc('% Bicycle', 'Workers >16', base)

#Vehicle Miles
rollup_percent_calc('Average weekday vehicle miles traveled per household', 'Household Count', base)

#Median Household Income
med_HH_income_scale = {'<10000': 0,
    '10000-14999': 1,
    '15000-24999': 2,
    '25000-34999': 3,
    '35000-49999': 4,
    '50000-74999': 5,
    '75000-99999': 6,
    '100000-149999': 7,
    '150000-199999': 8,
    '>200000': 9}
all_incomes = []
for m in med_HH_income_scale.keys():
    #get total number of people in each income bracket
    end = base.columns.get_loc('All Tracts')
    start = base.columns[0]
    stop = base.columns[end-1]
    stop_extra = base.columns[end]
    temp = 'Total ' + m
    base.loc[temp] = (base.loc[m, :]) * base.loc['Total with Income Data', :] / 100
    base.at[temp, 'All Tracts'] = base.loc[temp, start:stop].sum(axis=0)
    
base.astype({'All Tracts': object})
    
#add corresponding values to all_incomes
for m in med_HH_income_scale.keys():
    num = round(base.loc['Total ' + m, 'All Tracts'])
    arr = num * [med_HH_income_scale[m]]
    all_incomes.extend(arr)

med_scale = statistics.median(all_incomes)
for m in med_HH_income_scale.keys():
    if med_scale == med_HH_income_scale[m]:
        med_band = m

base.at['Median Household Income', 'All Tracts'] = med_scale

#adjust the state number to be a band
state_med = base.at['Median Household Income', State.code]
for m in med_HH_income_scale.keys():
    if m == '<10000':
        if state_med <= 10000:
            base.at['Median Household Income', State.code] = med_HH_income_scale[m]
    elif m == '>200000':
        if state_med >= 200000:
            base.at['Median Household Income', State.code] = med_HH_income_scale[m]
    else:
        split = m.split('-')
        low = float(split[0])
        high = float(split[1])
        if state_med >= low and state_med <= high:
            base.at['Median Household Income', State.code] = med_HH_income_scale[m]


#Poverty Rate
base.loc['Poverty Rate (%)', 'All Tracts'] = (base.loc['Below Poverty Level', 'All Tracts']) \
    / base.loc['Total with Poverty Data', 'All Tracts'] * 100

#Unemployment Rate
base.loc['Unemployment Rate (%)', 'All Tracts'] = (base.loc['Total Unemployed', 'All Tracts']) \
    / base.loc['In Labor Force', 'All Tracts'] * 100

#Educational Attainment
#add up the actual numbers, this is all correct
base.loc['Associates, Bachelors degree, or higher (%)', :] = ((base.loc['> 25 with Associates', :]) + (base.loc['> 25 with Bachelors or higher', :])) \
    / base.loc['Total with Education Data >25', :]
#take care of margins of error
base.loc['Associates, Bachelors degree, or higher (%)', start_m:stop_m:2] = add_moe(['> 25 with Associates', '> 25 with Bachelors or higher'], start_m, stop_m, 2, base)
base.loc['Associates, Bachelors degree, or higher (%)', start_m:stop_m:2] = divide_moe('Associates, Bachelors degree, or higher (%)', 'Total with Education Data >25', 'Associates, Bachelors degree, or higher (%)', start_m, stop_m, 2, start_e, stop_e, base)
base.loc['Associates, Bachelors degree, or higher (%)', :] = base.loc['Associates, Bachelors degree, or higher (%)', :] * 100

# #state level
# base.loc['Associates, Bachelors degree, or higher (%)', 'MOE' + State.code] = add_moe(['> 25 with Associates', '> 25 with Bachelors or higher'], 'MOE' + State.code, base.columns[end], 1, base)
# base.loc['Associates, Bachelors degree, or higher (%)', 'MOE' + State.code] = divide_moe('Associates, Bachelors degree, or higher (%)', 'Total with Education Data >25', 'Associates, Bachelors degree, or higher (%)', 'MOE' + State.code, base.columns[end], 1, State.code, 'MOE' + State.code, base)
# base.loc['Associates, Bachelors degree, or higher (%)', :] = base.loc['Associates, Bachelors degree, or higher (%)', :] * 100


#Limited English-speaking Households
base.loc['Limited English-speaking Households (%)', 'All Tracts'] = (base.loc['Limited English-speaking', 'All Tracts']) \
    / base.loc['Total with Language Data', 'All Tracts'] * 100

#Population of Color
base.loc['Population of Color (%)', :] = (base.loc['Total with Race Data', :] - base.loc['Total White Alone', :]) \
    / base.loc['Total with Race Data', :]
base.loc['Population of Color (%)', start_m:stop_m:2] = add_moe(['Total with Race Data', 'Total White Alone'], start_m, stop_m, 2, base)
base.loc['Population of Color (%)', start_m:stop_m:2] = divide_moe('Population of Color (%)', 'Total with Race Data', 'Population of Color (%)', start_m, stop_m, 2, start_e, stop_e, base)
base.loc['Population of Color (%)', :] = base.loc['Population of Color (%)', :] * 100

#Cost-Burdened Renters
base.loc['Cost-Burdened Renters (%)', :] = (base.loc['Rent 30.0-34.9%', :] + base.loc['Rent 35.0-39.9%', :] \
    + base.loc['Rent 40.0-49.9%', :] + base.loc['Rent >50.0%', :]) \
    / base.loc['Total with Rent Data', :]
base.loc['Cost-Burdened Renters (%)', start_m:stop_m:2] = add_moe(['Rent 30.0-34.9%', 'Rent 35.0-39.9%', 'Rent 40.0-49.9%', 'Rent >50.0%'], start_m, stop_m, 2, base)
base.loc['Cost-Burdened Renters (%)', start_m:stop_m:2] = divide_moe('Cost-Burdened Renters (%)', 'Total with Rent Data', 'Cost-Burdened Renters (%)', start_m, stop_m, 2, start_e, stop_e, base)
base.loc['Cost-Burdened Renters (%)', :] = base.loc['Cost-Burdened Renters (%)', :] * 100
  
#Low-Income <5
base.loc['Population of Low-Income Children <5 (%)', 'All Tracts'] = (base.loc['Low-Income <5', 'All Tracts']) \
    / base.loc['Total <5', 'All Tracts'] * 100

#Low-Income >65
base.loc['Population of Low-Income Seniors >65 (%)', 'All Tracts'] = (base.loc['Low-Income >65', 'All Tracts']) \
    / base.loc['Total >65', 'All Tracts'] * 100
    
#Car Ownership
base.loc['Average Number of Cars Per Household', :] = (base.loc['No vehicles', :] * 0 \
    + base.loc['1 vehicle', :] * 1 \
    + base.loc['2 vehicles', :] * 2 \
    + base.loc['>3 vehicles', :] * 3) / (base.loc['Occupied Housing Units', :])



print(base)
#MOE Aggregation
base = agg_moe(base)

#Ratio
base['Ratio'] = base['All Tracts'] / base[State.code]
base.at['Median Household Income', 'Ratio'] = base.at['Median Household Income', 'All Tracts'] - base.at['Median Household Income', State.name]

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
             'Transit Frequency', \
             'Median Household Income', \
             'Poverty Rate (%)', \
             'Unemployment Rate (%)', \
             'Associates, Bachelors degree, or higher (%)', \
             'Limited English-speaking Households (%)', \
             'Population of Color (%)', \
             'Cost-Burdened Renters (%)', \
             'Population of Low-Income Children <5 (%)', \
             'Population of Low-Income Seniors >65 (%)', \
             'School Performance - Overall', \
             'School Performance - Econ. Disadvantaged', \
             'Average Number of Cars Per Household']
base = base.reindex(row_order)

#EXCEL SHEET
sheet_title = 'HealthScore 2.0'
writer = pd.ExcelWriter(project + '.xlsx', engine='xlsxwriter')   
workbook=writer.book
worksheet=workbook.add_worksheet(sheet_title)
writer.sheets[sheet_title] = worksheet
base.to_excel(writer,sheet_name=sheet_title,startrow=0 , startcol=0)   
writer.save()

print('DONE')