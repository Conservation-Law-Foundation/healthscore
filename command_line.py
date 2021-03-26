#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 11:51:44 2021

@author: emilyfang
"""
import pandas as pd
import numpy as np
import requests

pd.set_option('display.max_columns', None)
pd.set_option('expand_frame_repr', False)
pd.options.display.max_colwidth = 100

from sodapy import Socrata

# # Unauthenticated client only works with public data sets. Note 'None'
# # in place of application token, and no username or password:
# client = Socrata("chronicdata.cdc.gov", None)

# # Example authenticated client (needed for non-public datasets):
# # client = Socrata(chronicdata.cdc.gov,
# #                  MyAppToken,
# #                  userame="user@example.com",
# #                  password="AFakePassword")

# # First 2000 results, returned as JSON from API / converted to Python list of
# # dictionaries by sodapy.
# results = client.get("cwsq-ngmh", year="2018", locationid='25021421100', measureid='CANCER', limit=2000)

# # Convert to pandas DataFrame
# results_df = pd.DataFrame.from_records(results)
# print(results_df)

# df = pd.read_excel('MA_DATA.xlsx', index_col=0)
# print(df)

# response = requests.get('https://ejscreen.epa.gov/mapper/ejscreenRESTbroker.aspx?namestr=25021420302&geometry=&distance=&unit=9035&areatype=tract&areaid=25021420302&f=pjson')
# data = response.json() #oh this is already a dictionary kek
# df = pd.DataFrame.from_dict(data)
# print(df)
#print(data)

# df = pd.DataFrame(index=['A','B','C'], columns=['x','y'])

# df.at['C', 'x'] = 10
# print(df)

# df = pd.read_excel('accountability-data-2019.xlsx', sheet_name='Table 3 - Schools', usecols='D,L', header=0)
# df.columns = df.iloc[0]
# df = df[1:]
# print(df)

# df = pd.read_excel('nextgenacct.xls', header=2)
# # df.columns = df.iloc[1]
# # df = df[2:]
# print(df)

# edu = pd.read_excel('Accountability_201819.xlsx', sheet_name='School Indicator Data')
# #calculate 67th percentile
# all_districts = edu['District'].unique()
# all_districts = all_districts[1:]
# all_scores = []
# for d in all_districts:
#     #d = str(d)
#     score = edu.loc[(edu['District'] == d) & (edu['Group'] == 'All Students')]['2019 Star Rating'].mean(axis=0)
#     all_scores.append(score)

# cutoff = np.nanpercentile(all_scores, 67)
# base.at['School Performance - Overall', 'All Tracts'] = edu.loc[edu['District'] == district & edu['Group'] == 'All Students']['2019 Star Rating']
    
# import os
# print(os.getcwd())


# import shapefile 
# import geopandas as gpd

# # myshp = open("OpenSpacePerCapita_2015_MAPC/OpenSpacePerCapita_2015_MAPC.shp", "rb")
# # mydbf = open("OpenSpacePerCapita_2015_MAPC/OpenSpacePerCapita_2015_MAPC.dbf", "rb")
# # r = shapefile.Reader(shp=myshp, dbf=mydbf)

# data = gpd.read_file("OpenSpacePerCapita_2015_MAPC/OpenSpacePerCapita_2015_MAPC.shp")



#urban_tracts = urban_tracts.str[1:]

    

#epa_urban = epa.loc[epa['GEOID10'].astype(str).str[1:] == urban_tracts.astype(str)]
#plt.hist(epa['D4c'], 100)

iterables = [["bar", "baz", "foo", "qux"], ["one", "two"]]

index = pd.MultiIndex.from_product(iterables, names=["first", "second"])
s = pd.DataFrame(np.random.randn(8,8), columns=index)
print(s)
