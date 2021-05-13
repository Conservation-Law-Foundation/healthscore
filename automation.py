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
from sodapy import Socrata
import statistics

# # PANDAS SETTINGS - DO NOT EDIT
#pd.set_option('display.max_columns', 10)
# pd.set_option('expand_frame_repr', False)
# pd.options.display.float_format = '{:.2f}'.format #set number of decimal points
pd.set_option('mode.chained_assignment',None)

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
# district = str(input("District: " ))
# community = str(input('Community: '))
# project = str(input("Project Name: " ))


# #HARD CODED TRACT INPUT - HOLBROOK
# state = str(25)
# county = str(0)+str(21)
# #tract = str(421100)
# primary = '421100'
# tracts = ['421100','420302','421200']
# project = 'Holbrook sanity check3'
# block = str(4)
# district = 'Holbrook'
# community = 'CoN'

# #HARD CODE HAMILTON
# state = str(25)
# county = str(0)+ str(0) + str(9)
# #tract = str(421100)
# primary = '215102'
# tracts = ['215102', '215101', '216100']
# block = str(4)
# project = 'Hamilton automatic z score reading'
# district = 'Hamilton-Wenham'
# community = "CoO"

# # #HARD CODED TRACT INPUT - DOT AVE
# state = str(25)
# county = str(0)+str(25)
# #tract = str(421100)
# primary = '091600'
# tracts = ['091600','091700','092000','092101','092200']
# project = 'Dot Ave full output'
# block = str(1)
# district = 'Boston'
# community = "CoN"

# #HARD CODED TRACT INPUT - HRCF
state = str(44)
county = str(0)+str(0)+str(7)
#tract = str(421100)
primary = '000700'
tracts = ['000700']
project = 'HRCF - CoN'
block = str(3)
district = 'Boston'
community = "CoN"


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
    col.append(T.code) #add tract names to columns

#MAKE COLUMNS
#add 'All Tracts' and State to columns
temp = ['All Tracts', State.code]
col.extend(temp)
#second level
col2 = ["EST", "SE", "MOE", "PERC"]
iterables = [col, col2]
index = pd.MultiIndex.from_product(iterables)

#MAKE ROWS


ind = ['Life Expectancy', \
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
              'Urban Group', \
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

#MAKE THE DATAFRAME
an_array = np.empty((len(ind),len(col)*len(col2)))
an_array[:] = np.NaN
base = pd.DataFrame((an_array), index=ind, columns=index, dtype='object')

# other_cols = ['Source']
# another_array = np.empty((len(ind),len(other_cols)))
# another_array[:] = np.NaN

# # base = base.join(pd.DataFrame((another_array), index=ind, columns=other_cols, dtype='object'))
# base['Source'] = another_array

############
#DATA PULLS#
############
import databases

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

# # #EPA SMART LOCATION
# epa = pd.read_csv('epa_smartlocation.csv')
# geoid = int(state + county + primary + block)
# print(geoid)
# base.loc['Transit Frequency', ('All Tracts', 'EST')] =  epa.loc[epa['GEOID10'] == geoid]['D4c'].values[0]
# base.loc['Transit Frequency', 'Source'] = 'EPA'

#LATCH
LATCH = databases.LATCH
add_latch_data(Tracts, LATCH, base)
#LATCH state data processing
#urb = base.loc['Urban Group', base.columns[0]]
ser = LATCH.data['geocode'].astype(str)
bools = ser.str.startswith(state)
inds = bools[bools].index
state_data = LATCH.data.loc[inds]
state_data = state_data.astype({'urban_group': 'Int64'})
#base.at['Average weekday vehicle miles traveled per household', State.code] = state_data.loc[state_data['urban_group'] == urb]['est_vmiles'].mean(axis=0)


state_miles = 0
for t in Tracts:
    urb = base.loc['Urban Group', (t.code, 'EST')]
    state_miles  += state_data.loc[state_data['urban_group'] == urb]['est_vmiles'].mean(axis=0)
base.at['Average weekday vehicle miles traveled per household', (State.code, 'EST')] = state_miles / len(tracts)

# #MA DATA FROM OTHER SOURCES
MA_data = pd.read_excel('MA_DATA.xlsx', index_col=0)
for i in MA_data.index:
    base.loc[i, (State.code, 'EST')] = MA_data.loc[i]['E']
    base.loc[i, (State.code, 'MOE')] = MA_data.loc[i]['M']

#SCHOOL DATA
#add_schools(state, district, base)

 
##############
#CALCULATIONS#
##############

#AGGREGATE MOE
agg_moe(tracts, base)

#AGGREGATE EST
for index, row in base['All Tracts'].iterrows():
    if pd.isna(row['EST']):
        sums = 0
        for t in tracts:
            sums += base.loc[index, (t, 'EST')]
        base.loc[index, ('All Tracts', 'EST')] = sums

#Life Expectancy
rollup_num_calc('Life Expectancy', 'Total Population', base, tracts, state, col)
sums = 0
for t in tracts:
    sums += base.loc['Life Expectancy', (t, 'SE')] **2
base.loc['Life Expectancy', ('All Tracts', 'SE')] = sums**(1/2)
base.loc['Life Expectancy', ('All Tracts', 'MOE')] = base.loc['Life Expectancy', ('All Tracts', 'SE')] * 1.645

#PLACES calculations
base.loc['PLACES population', (['All Tracts', State.code], 'MOE')] = 0
base.loc['PLACES population', (State.code, 'EST')] = 88929
for m in PLACES.metrics_dict.keys():
    rollup_percent_calc(m, 'PLACES population', base, tracts, state, col)
    #base.loc[m, ('All Tracts', 'SE')] = base['All Tracts'].loc[m, 'MOE'] / 1.96
    #base.loc[m, (State.code, 'SE')] = base[State.code].loc[m, 'MOE'] / 1.96
    # divide_moe(m, 'Total ' + m, 'PLACES population', m, base, ['All Tracts'])

# # #EJ Screen calculations
for m in EJ.metrics_dict.keys():
    row_avg(m, base, tracts)
    perc_avg(m, base, tracts)

#Transit Use
rollup_percent_calc('% Public Transit', 'Workers >16', base, tracts, state, col)
rollup_percent_calc('% Walked', 'Workers >16', base, tracts, state, col)
rollup_percent_calc('% Bicycle', 'Workers >16', base, tracts, state, col)

#Vehicle Miles
rollup_num_calc('Average weekday vehicle miles traveled per household', 'Household Count', base, tracts, state, col)

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
#get total number of people in each income bracket
for m in med_HH_income_scale.keys():
    temp = 'Total ' + m
    sums = 0
    for t in tracts:
        sums += base.loc[m, (t, 'EST')] * base.loc['Total with Income Data', (t, 'EST')] / 100
    base.loc[temp, ('All Tracts', 'EST')] = round(sums)
    
    # #add corresponding values to all_incomes
    num = base.loc[temp, ('All Tracts', 'EST')]
    arr = num * [med_HH_income_scale[m]]
    all_incomes.extend(arr)

med_scale = statistics.median(all_incomes)
for m in med_HH_income_scale.keys():
    if med_scale == med_HH_income_scale[m]:
        med_band = m

base.loc['Median Household Income', ('All Tracts', 'EST')] = med_scale
base.astype({'All Tracts': object})

#adjust the state number to be a band
state_med = base.loc['Median Household Income', (State.code, 'EST')]
for m in med_HH_income_scale.keys():
    if m == '<10000':
        if state_med <= 10000:
            base.loc['Median Household Income', (State.code, 'EST')] = med_HH_income_scale[m]
    elif m == '>200000':
        if state_med >= 200000:
            base.at['Median Household Income', (State.code, 'EST')] = med_HH_income_scale[m]
    else:
        split = m.split('-')
        low = float(split[0])
        high = float(split[1])
        if state_med >= low and state_med <= high:
            base.loc['Median Household Income', (State.code, 'EST')] = med_HH_income_scale[m] 

# SIMPLE DIVISION OF SOME METRICS
divide_rows('Poverty Rate (%)', 'Below Poverty Level', 'Total with Poverty Data', 'Poverty Rate (%)', base, col)
divide_rows('Unemployment Rate (%)', 'Total Unemployed', 'In Labor Force', 'Unemployment Rate (%)', base, col)
divide_rows('Limited English-speaking Households (%)', 'Limited English-speaking', 'Total with Language Data', 'Limited English-speaking Households (%)', base, col)
divide_rows('Population of Low-Income Children <5 (%)', 'Low-Income <5', 'Total <5', 'Population of Low-Income Children <5 (%)', base, col)
divide_rows('Population of Low-Income Seniors >65 (%)', 'Low-Income >65', 'Total >65', 'Population of Low-Income Seniors >65 (%)', base, col)

# # #Educational Attainment
add_est('Total Educated', ['> 25 with Associates', '> 25 with Bachelors or higher'], base, col)
divide_est('Associates, Bachelors degree, or higher (%)', 'Total Educated', 'Total with Education Data >25', base, col)
add_moe('Total Educated', ['> 25 with Associates', '> 25 with Bachelors or higher'], base, col)
divide_moe('Associates, Bachelors degree, or higher (%)', 'Total Educated', 'Total with Education Data >25', 'Associates, Bachelors degree, or higher (%)', base, col)

# # #Population of Color
base.loc['Total with Race Data', (State.code, 'MOE')] = 0
subtract_est('Total Color', 'Total with Race Data', 'Total White Alone', base, col)
divide_est('Population of Color (%)', 'Total Color', 'Total with Race Data', base, col)
add_moe('Total Color', ['Total with Race Data', 'Total White Alone'], base, col)
divide_moe('Population of Color (%)', 'Total Color', 'Total with Race Data', 'Population of Color (%)', base, col)

# # #Cost-Burdened Renters
cbr_list = ['Rent 30.0-34.9%', 'Rent 35.0-39.9%', 'Rent 40.0-49.9%', 'Rent >50.0%']
add_est('Total CBR', cbr_list, base, col)
divide_est('Cost-Burdened Renters (%)', 'Total CBR', 'Total with Rent Data', base, col)
add_moe('Total CBR', cbr_list, base, col)
divide_moe('Cost-Burdened Renters (%)', 'Total CBR', 'Total with Rent Data', 'Cost-Burdened Renters (%)', base, col)

# #Car Ownership
assign = (col, 'EST')
base.loc['Average Number of Cars Per Household', assign] = (base.loc['No vehicles', assign] * 0 \
   + base.loc['1 vehicle', assign] * 1 \
   + base.loc['2 vehicles', assign] * 2 \
   + base.loc['>3 vehicles', assign] * 3) / (base.loc['Occupied Housing Units', assign])
 


# # #MARGIN OF ERROR TO STANDARD ERROR
# for index, row in base['All Tracts'].iterrows():
#     if pd.isna(row['SE']) and not pd.isna(row['MOE']):
#          base.loc[index, ('All Tracts', 'SE')] = base['All Tracts']['MOE'][index]

#Z SCORES
logic = pd.read_excel('logic.xlsx', index_col=0)
for i in logic.index:
    base.loc[i, 'CoO'] = logic.loc[i]['CoO'].astype(bool)
    base.loc[i, 'CoN'] = logic.loc[i]['CoN'].astype(bool)

for index, row in base['All Tracts'].iterrows():
    if base.loc[index, 'Source'].to_numpy() == 'PLACES':
        z = 1.96
    elif base.loc[index, 'Source'].to_numpy() == 'LATCH':
        pass
    else:
        z = 1.645
    X1 = base.loc[index, ('All Tracts', 'EST')] / 100
    X2 = base.loc[index, (State.code, 'EST')] / 100
    SE1 = (base.loc[index, ('All Tracts', 'MOE')] / 100)/ z
    SE2 = (base.loc[index, (State.code, 'MOE')] / 100) / z
    if (SE1**2 + SE2**2)**(1/2) == 0:
        pass
    else:
        base.loc[index, 'Z Score'] =  (X1 - X2) / (SE1**2 + SE2**2)**(1/2)
    #base.loc[index, 'Z Score'] =  (X1 - X2) / (SE1**2 + SE2**2)**(1/2)
    z_score = base.loc[index, 'Z Score'].values
    
    if abs(z_score) < 1.282:
        base.loc[index, '% Points'] = 0.5
    
    elif base.loc[index, community].values:
        if z_score < -1.282:
            base.loc[index, '% Points'] = 0
        elif z_score > 1.282 and z_score < 1.645:
            base.loc[index, '% Points'] = 2/3
        elif z_score > 1.645 and z_score < 2.326:
            base.loc[index, '% Points'] = 5/6
        elif z_score > 2.326:
            base.loc[index, '% Points'] = 1
    
    elif not base.loc[index, community].values:
        if z_score > 1.282:
            base.loc[index, '% Points'] = 0
        elif z_score < -1.282 and z_score > -1.645:
            base.loc[index, '% Points'] = 2/3
        elif z_score < -1.645 and z_score > -2.326:
            base.loc[index, '% Points'] = 5/6
        elif z_score < -2.326:
            base.loc[index, '% Points'] = 1
  
base = base.rename({'% Points': '% of Max Points Scored (Decimal Value)'}, axis='columns')

#Urban Group
primary_group = base.loc['Urban Group', (primary, 'EST')]
base.loc['Urban Group', ('All Tracts', 'EST')] = primary_group
if primary_group == 1:
    base.loc['Urban Group', ('All Tracts', 'EST')] = 'URB'
elif primary_group == 2:
    base.loc['Urban Group', ('All Tracts', 'EST')] = 'SUB'
elif primary_group == 3:
    base.loc['Urban Group', ('All Tracts', 'EST')] = 'RUR'
    
# # #Ratio
base['Ratio'] = base['All Tracts']['EST'] / base[State.code]['EST']
base.at['Median Household Income', 'Ratio'] = base.at['Median Household Income', ('All Tracts', 'EST')] - base.at['Median Household Income', (State.code, 'EST')]

#print(base)

# # # ###########
# # # #TOUCH-UPS#
# # # ###########

# # # # #remove rows
# # # # remove_list = ['Total with Race Data', 'Total White Alone', \
# # # #                 'Total with Rent Data', 'Rent 30.0-34.9%', 'Rent 35.0-39.9%', 'Rent 35.0-39.9%', 'Rent 40.0-49.9%', 'Rent >50.0%']
# # # # base.drop(remove_list, axis=0, inplace=True)


# #reorder rows
if community == 'CoN':
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
                  'Urban Group', \
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
                  'Average Number of Cars Per Household']
elif community == 'CoO':
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
                  'Urban Group', \
                  'Transit Frequency', \
                  'Median Household Income', \
                  'Poverty Rate (%)', \
                  'Unemployment Rate (%)', \
                  'Associates, Bachelors degree, or higher (%)', \
                  'Cost-Burdened Renters (%)', \
                  'Population of Low-Income Children <5 (%)', \
                  'Population of Low-Income Seniors >65 (%)', \
                  'School Performance - Overall', \
                  'School Performance - Econ. Disadvantaged', \
                  'Average Number of Cars Per Household']  
base = base.reindex(row_order)

#print(base)

# #EXCEL SHEET
# sheet_title = 'HealthScore 2.0'
# writer = pd.ExcelWriter(project + '.xlsx', engine='xlsxwriter')   
# workbook=writer.book
# worksheet=workbook.add_worksheet(sheet_title)
# writer.sheets[sheet_title] = worksheet
# base.to_excel(writer,sheet_name=sheet_title,startrow=0 , startcol=0)   
# writer.save()
#name = project + '.xlsx'
base.to_excel('output.xlsx')

print('DONE')