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

# # #HARD CODED TRACT INPUT - HRCF
# state = str(44)
# county = str(0)+str(0)+str(7)
# #tract = str(421100)
# primary = '000700'
# tracts = ['000700']
# project = 'HRCF - CoN'
# block = str(3)
# district = 'Providence'
# community = "CoO"

# # #HARD CODED TRACT INPUT - HRCF
# state = str(0) + str(9)
# county = str(0)+str(0)+str(9)
# #tract = str(421100)
# primary = '361401'
# tracts = ['361401']
# block = str(3)
# project = 'CT trial'
# district = 'New Haven School District'
# community = "CoO"