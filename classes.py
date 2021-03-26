#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Jan 14 16:13:01 2021

@author: emilyfang
"""
#CLASSES - DO NOT EDIT
class Place:
    def __init__(self, name, code):
        self.name = name + code
        self.code = code
      
class Metric:
    def __init__(self, name, code):
        self.name = name
        self.code = code
    def update_codes(self):
        self.code_E = str(self.code) + 'E'
        self.code_M = str(self.code) + 'M'
    def update_EJ_codes(self):
        self.code_E = 'RAW_E_' + str(self.code)
        self.code_S = 'S_E_' + str(self.code)
        self.code_P = 'S_P_' + str(self.code)
        
class Database:
    def __init__(self, name):
        self.name = name
        self.metrics_dict = {}
        self.metrics = [] #list of Metrics
        self.call_base = {}
        self.call_end_tract = {}
        self.call_end_state = {}
        self.call_tract = []
        self.call_state = []
        self.data = []
    def update_metrics(self):
        for k in self.metrics_dict.keys():
            self.metrics.append(Metric(k, self.metrics_dict[k]))
        for m in self.metrics:
            m.update_codes()
    def update_EJ_metrics(self):
        for k in self.metrics_dict.keys():
            self.metrics.append(Metric(k, self.metrics_dict[k]))
        for m in self.metrics:
            m.update_EJ_codes()
    