#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 16:19:11 2021

@author: emilyfang
"""
import requests
import pandas as pd
import numpy as np
from sodapy import Socrata
import os

from helpers import *
from classes import *
import automation


state = automation.state
county = automation.county
tracts = automation.tracts
project = automation.project

KEY = 'f9f363d3f234e17efe942546146445b2a9395a1d'

# #METRICS
# MED_HH_INC = Metric('MED_HH_INC', 'Median Household Income', 'B19013_001', True, False)
# RACE_TOT = Metric('RACE_TOT', 'Total with Race Data', 'B02001_001', [], [])
# WHITE_TOT = Metric('WHITE_TOT', 'Total White Alone', 'B02001_002', [], [])

# #Cost-Burdened Renters
# RENT_TOT = Metric('RENT_TOT', 'Total with Rent Data', 'B25070_001', [], [])
# RENT_30_34 = Metric('RENT_30_34', 'Rent 30.0-34.9%', 'B25070_007', [], [])
# RENT_35_39 = Metric('RENT_35_39', 'Rent 35.0-39.9%', 'B25070_008', [], [])
# RENT_40_49 = Metric('RENT_40_49', 'Rent 40.0-49.9%', 'B25070_009', [], [])
# RENT_50U = Metric('RENT_50U', 'Rent >50.0%', 'B25070_010', [], [])
# POP_TOT = Metric('POP_TOT', 'Total Population', 'B01003_001', [], [])


#CANCER = Metric('Cancer (excluding skin cancer) among adults >= 18', 'CANCER')


#DATABASES - ONLY EDIT IF CHANGING METRICS

#ACS DETAILED TABLE
ACS_B = Database('ACS Detailed')
#ACS_B.metrics = [MED_HH_INC, RACE_TOT, WHITE_TOT, RENT_TOT, RENT_30_34, RENT_35_39, RENT_40_49, RENT_50U, POP_TOT]
ACS_B.metrics_dict = {
    'Median Household Income': 'B19013_001',
    
    #Population of Color
    'Total with Race Data': 'B02001_001',
    'Total White Alone': 'B02001_002',
    
    #Cost Burdened Renters
    'Total with Rent Data': 'B25070_001',
    'Rent 30.0-34.9%': 'B25070_007',
    'Rent 35.0-39.9%': 'B25070_008',
    'Rent 40.0-49.9%': 'B25070_009',
    'Rent >50.0%': 'B25070_010', 
    
    #Population
    'Total Population': 'B01003_001'
    }
#ACS_B.metrics_dict = {}
# for m in ACS_B.metrics:
#     ACS_B.metrics_dict[m.name] = m.call
ACS_B.call_base = "https://api.census.gov/data/2019/acs/acs5?get=NAME,"
ACS_B.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county + "&key=" + KEY
ACS_B.call_end_state = "&for=state:" + state + "&key=" + KEY

#ACS SUBJECT TABLE
ACS_S = Database('ACS Subject')
ACS_S.metrics_dict = {
    #Median Household Income
    'Total with Income Data': 'S1901_C01_001',
    '<10000': 'S1901_C01_002',
    '10000-14999': 'S1901_C01_003',
    '15000-24999': 'S1901_C01_004',
    '25000-34999': 'S1901_C01_005',
    '35000-49999': 'S1901_C01_006',
    '50000-74999': 'S1901_C01_007',
    '75000-99999': 'S1901_C01_008',
    '100000-149999': 'S1901_C01_009',
    '150000-199999': 'S1901_C01_010',
    '>200000': 'S1901_C01_011',
    
    #Poverty Rate
    #'Poverty Rate': 'S1701_C03_001',
    'Total with Poverty Data': 'S1701_C01_001',
    'Below Poverty Level': 'S1701_C02_001',
    'Poverty Rate (%)': 'S1701_C03_001',
    
    #Education Attainment
    # '% >25 with Associates': 'S1501_C02_011',
    # '% >25 with Bachelors or higher': 'S1501_C02_015',
    'Total with Education Data >25': 'S1501_C01_006',
    '> 25 with Associates' : 'S1501_C01_011',
    '> 25 with Bachelors or higher' : 'S1501_C01_015',
    
    #Limited English Households
    'Limited English-speaking Households (%)': 'S1602_C04_001',
    'Total with Language Data': 'S1602_C01_001',
    'Limited English-speaking': 'S1602_C03_001',
    
    #Low-Income <5
    'Low-Income <5': 'S1701_C02_003',
    'Total <5': 'S1701_C01_003',
    'Population of Low-Income Children <5 (%)': 'S1701_C03_003',
    
    #Low-Income >65
    'Low-Income >65': 'S1701_C02_010',
    'Total >65': 'S1701_C01_010',
    'Population of Low-Income Seniors >65 (%)': 'S1701_C03_010',
    
    #Transit Use
    'Workers >16': 'S0801_C01_001',
    '% Public Transit': 'S0801_C01_009',
    '% Walked': 'S0801_C01_010',
    '% Bicycle': 'S0801_C01_011'
    }
ACS_S.call_base = "https://api.census.gov/data/2019/acs/acs5/subject?get=NAME,"
ACS_S.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county + "&key=" + KEY
ACS_S.call_end_state = "&for=state:" + state + "&key=" + KEY

#ACS DATA PROFILES
ACS_D = Database('ACS Data Profiles')
ACS_D.metrics_dict = {
    #Unemployment Rate
    'Unemployment Rate (%)': 'DP03_0009P',
    'In Labor Force': 'DP03_0002',
    'Total Unemployed': 'DP03_0005',
    #Car Ownership
    'Occupied Housing Units': 'DP04_0057',
    'No vehicles': 'DP04_0058',
    '1 vehicle': 'DP04_0059',
    '2 vehicles': 'DP04_0060',
    '>3 vehicles': 'DP04_0061'
    }
ACS_D.call_base = "https://api.census.gov/data/2019/acs/acs5/profile?get=NAME,"
ACS_D.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county + "&key=" + KEY
ACS_D.call_end_state = "&for=state:" + state + "&key=" + KEY

#ACS 2015 DETAILED TABLE
ACS_2015 = Database('ACS Detailed 2015')  
ACS_2015.metrics_dict = {
    'Total Population': 'B01003_001'
    }
ACS_2015.call_base = "https://api.census.gov/data/2015/acs/acs5?get=NAME,"
ACS_2015.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county + "&key=" + KEY
ACS_2015.call_end_state = "&for=state:" + state + "&key=" + KEY

dbs = [ACS_B, ACS_S, ACS_D, ACS_2015]

#RWJF LIFE EXPECTANCY
CDC = Database('CDC')
CDC.data = pd.read_csv('rwjf.csv')
CDC.metrics = ['Life Expectancy']

#CDC PLACES
PLACES = Database('PLACES')
PLACES.metrics_dict = {
    'Cancer (excluding skin cancer) among adults >= 18': 'CANCER',
    'Current asthma among adults >= 18': 'CASTHMA',
    'COPD among adults >= 18': 'COPD',
    'Coronary heart disease among adults >= 18': 'CHD',
    'Diabetes among adults >= 18': 'DIABETES',
    'Stroke among adults >= 18': 'STROKE',
    'Mental health not good for >= 14 days among adults >= 18': 'MHLTH'}
client = Socrata("chronicdata.cdc.gov", None)

#EJ SCREEN
EJ = Database('EJ')
EJ.metrics_dict = {
    'PM 2.5 (ug/m3)': 'PM25',
    'NATA Diesel PM (ug/m3)': 'DIESEL',
    'NATA Air Toxics Cancer Risk (risk per MM)': 'CANCER',
    'NATA Respiratory Hazard Index': 'RESP'
    }
EJ.call_base = 'https://ejscreen.epa.gov/mapper/ejscreenRESTbroker.aspx?namestr=' + 'XXXXXXXXXXX' + '&geometry=&distance=&unit=9035&areatype=tract&areaid=' + 'XXXXXXXXXXX' +'&f=pjson'

#LATCH
LATCH = Database('LATCH')
LATCH.data = pd.read_csv('latch.csv')
LATCH.metrics_dict = {'Average weekday vehicle miles traveled per household': 'est_vmiles', \
                      'Household Count': 'hh_cnt',
                      'Urban Group': 'urban_group'}





