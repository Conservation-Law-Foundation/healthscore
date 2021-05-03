#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 16:43:19 2021

@author: emilyfang
"""
import requests
import os
import pandas as pd
import numpy as np
from helpers import *
from classes import *
from automation import *
from databases import *


# #Holbrook: tracts are 421100, 420302, 421200

# Hamilton: 25, county: 009, tracts: 215102, 215101, 216100. district: Hamilton-Wenham

#Dot ave: 
    # # #HARD CODED TRACT INPUT - DOT AVE
# state = str(25)
# county = str(0)+str(25)
# primary = '091600'
# tracts = ['091600','091700','092000','092101','092200']
# project = 'Dot Ave full output'
# block = str(1)
# district = 'Boston'
# community = "CoN"