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
for t in tracts:
    T = Place(project, t)
    places.append(T)

#DATA PULLS
import databases
for db in databases.dbs:
    db.update_metrics()
    create_acs_calls(db, places)
    add_data(db, places)

#COMPILATION
df_proj = compile_data(places)

calcs.calculations(df_proj)
calcs.touchups_and_export(df_proj)
#print(df_proj)

# # #CALCULATIONS
# #Population of Color
# df['Population of Color (%)'] = (df['Total with Race Data']-df['Total White Alone']) / df['Total with Race Data'] * 100
# #Neighborhood Renters who are Cost-Burdened
# df['% Neighborhood Renters who are Cost-Burdened'] = (df['Rent 30.0-34.9%'] \
#                                                       + df['Rent 35.0-39.9%']
#                                                       + df['Rent 40.0-49.9%']
#                                                       + df['Rent >50.0%']) / df['Total with Rent Data'] * 100
# #Educational Attainment
# df['% with Associates/Bachelors Degree or higher'] = df['% >25 with Associates'] \
#                                                         + df['% >25 with Bachelors or higher']
    
# #REMOVE COLUMNS
# remove_list = ['Total with Race Data', 'Total White Alone', \
#                 'Total with Rent Data', 'Rent 30.0-34.9%', 'Rent 35.0-39.9%', 'Rent 35.0-39.9%', 'Rent 40.0-49.9%', 'Rent >50.0%']
# df.drop(remove_list, axis=1, inplace=True)

# #FINAL TOUCHUPS
# df = df[['% Public Transit', '% Walked', '% Bicycle', \
#           'Median Household Income', \
#           'Poverty Rate', \
#           'Unemployment Rate', \
#           '% with Associates/Bachelors Degree or higher', \
#           'Population of Color (%)', '% Limited English Households', \
#           '% Neighborhood Renters who are Cost-Burdened']]
# df = df.T
# print(df)