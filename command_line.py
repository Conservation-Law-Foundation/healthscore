#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Jan 13 11:51:44 2021

@author: emilyfang
"""
import pandas as pd
import numpy as np

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

df = pd.read_excel('MA_DATA.xlsx', index_col=0)
print(df)