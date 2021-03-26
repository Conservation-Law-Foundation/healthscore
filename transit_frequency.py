#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Feb 19 14:52:54 2021

@author: emilyfang
"""
import matplotlib.pyplot as plt
import pandas as pd

#datasets
latch = pd.read_csv('latch_2017-b.csv')
epa = pd.read_csv('MA_tracts_d4c.csv')
epa = epa.loc[epa['D4c'] != -99999.00] #remove negatives

#clean epa data (remove last digit of geoid)
epa_tracts = epa['GEOID10'].astype(str)
epa_tracts = epa_tracts.str[:-1]

def find_quartiles(group):
    #find tracts of that type
    tracts = latch.loc[latch['urban_group'] == group]['geocode']
    tracts = tracts.astype(str)
    
    #find matching epa tracts
    in_group = epa_tracts.isin(tracts)
    
    epa_group = epa[in_group]['D4c']
    return epa_group.quantile([0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1])


urban_groups = [1, 2, 3]

for i in urban_groups:
    print(find_quartiles(i))
    



    
    