#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 16:19:11 2021

@author: emilyfang
"""
import requests
import pandas as pd
import numpy as np
from helpers import *
from classes import *
import automation

state = automation.state
county = automation.county
tracts = automation.tracts
project = automation.project

#DATABASES - ONLY EDIT IF CHANGING METRICS

#ACS DETAILED TABLE
ACS_B = Database('ACS Detailed')  
ACS_B.metrics_dict = {
    'Median Household Income': 'B19013_001',
    #Population of Color
    'Total with Race Data': 'B02001_001',
    'Total White Alone': 'B02001_002',
    'Total with Rent Data': 'B25070_001',
    #Cost Burdened Renters
    'Rent 30.0-34.9%': 'B25070_007',
    'Rent 35.0-39.9%': 'B25070_008',
    'Rent 40.0-49.9%': 'B25070_009',
    'Rent >50.0%': 'B25070_010'
    }
ACS_B.call_base = "https://api.census.gov/data/2019/acs/acs5?get=NAME,"
ACS_B.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county
ACS_B.call_end_state = "&for=state:" + state

#ACS SUBJECT TABLE
ACS_S = Database('ACS Subject')
ACS_S.metrics_dict = {
    'Poverty Rate': 'S1701_C03_001',
    '% Limited English Households': 'S1602_C04_001',
    #Education Attainment
    '% >25 with Associates': 'S1501_C02_011',
    '% >25 with Bachelors or higher': 'S1501_C02_015',
    '% Public Transit': 'S0801_C01_009',
    '% Walked': 'S0801_C01_010',
    '% Bicycle': 'S0801_C01_011'
    }
ACS_S.call_base = "https://api.census.gov/data/2019/acs/acs5/subject?get=NAME,"
ACS_S.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county
ACS_S.call_end_state = "&for=state:" + state

#ACS DATA PROFILES
ACS_D = Database('ACS Data Profiles')
ACS_D.metrics_dict = {
    'Unemployment Rate': 'DP03_0009P'
    }
ACS_D.call_base = "https://api.census.gov/data/2019/acs/acs5/profile?get=NAME,"
ACS_D.call_end_tract = "&for=tract:" + "XXXXXX" + "&in=state:" + state + "+county:" + county
ACS_D.call_end_state = "&for=state:" + state

dbs = [ACS_B, ACS_S, ACS_D]

#CDC LIFE EXPECTANCY
CDC = Database('CDC')
CDC.data = pd.read_csv('MA_A.CSV')
CDC.metrics = ['Life Expectancy']
