#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 30 10:25:14 2019

@author: lee1984
"""

import sys

import numpy
import pandas

df = pandas.read_parquet('test.pqt')

#%% function body
column_info = pandas.DataFrame(df.dtypes.astype(str)).reset_index().rename(columns = {'index': 'column', 0: 'type'})
column_info['type'] = numpy.where(column_info['type'].str[:3].isin(['int', 'flo']), 'numeric', column_info['type'])

missing = pandas.DataFrame.from_dict({i: df[i].isnull().any() for i in df.columns}, orient = 'index')
missing = missing.reset_index().rename(columns = {'index': 'column', 0: 'missing'})

column_info = column_info.merge(missing, how = 'left', on = 'column')

in_data_numeric = df.select_dtypes(include = ['number'])
if len(in_data_numeric.columns) > 0:
    desc_numeric = in_data_numeric.describe(percentiles = [], include = 'all').transpose().reset_index()
    desc_numeric = desc_numeric.rename(columns = {'index': 'column'})[['column', 'min', 'max']]
    
    check_integer = {}
    for i in in_data_numeric.columns:
        check_integer[i] = in_data_numeric[i].apply(lambda x: float.is_integer(x / 1.0) or pandas.isnull(x)).all()
    
    del in_data_numeric
    
    check_integer = pandas.DataFrame.from_dict(check_integer, orient = 'index').reset_index()
    check_integer = check_integer.rename(columns = {'index': 'column', 0: 'is_integer'})
    
    column_info = column_info.merge(desc_numeric , how = 'left', on = 'column')
    column_info = column_info.merge(check_integer, how = 'left', on = 'column')
#    desc_numeric = desc_numeric.merge(check_integer, how = 'left', on = 'column')




















#ch_data_types = {
#    'contract_id':                    'String',
#    'objectno':                       'Nullable(String)',
#    'user_id':                        'Nullable(String)',
#    'cert_id':                        'Nullable(String)',
#    'credit_ratif_date':              'Nullable(Date)',
#    'putoutdate':                     'Nullable(Date)',
#    'credit_amount':                  'Nullable(Float64)',
#    'credit_term':                    'Nullable(UInt8)',
#    'loan_term':                      'Nullable(UInt8)',
#    'status':                         'Nullable(String)',
#    'spu_id':                         'Nullable(String)',
#    'sku_id':                         'Nullable(String)',
#    'max_dpd':                        'Nullable(UInt16)',
#    'max_dpd_3m':                     'Nullable(UInt16)',
#    'max_dpd_month':                  'Nullable(UInt16)',
#    'current_dpd':                    'Nullable(UInt16)',
#    'first_due_date':                 'Nullable(Date)',
#    'last_repay_date':                'Nullable(Date)',
#    'npd1':                           'Nullable(UInt8)',
#    'npd3':                           'Nullable(UInt8)',
#    'npd10':                          'Nullable(UInt8)',
#    'npd30':                          'Nullable(UInt8)',
#    'npd60':                          'Nullable(UInt8)',
#    'npd90':                          'Nullable(UInt8)',
#    'agr1':                           'Nullable(UInt8)',
#    'agr3':                           'Nullable(UInt8)',
#    'agr10':                          'Nullable(UInt8)',
#    'agr30':                          'Nullable(UInt8)',
#    'agr60':                          'Nullable(UInt8)',
#    'agr90':                          'Nullable(UInt8)',
#    'first_loss_date31':              'Nullable(Date)',
#    'first_loss_principle31':         'Nullable(Float64)',
#    'first_loss_interest31':          'Nullable(Float64)',
#    'first_loss_custsevicefee31':     'Nullable(Float64)',
#    'first_loss_insurancefee31':      'Nullable(Float64)',
#    'first_loss_vipcustsevicefee31':  'Nullable(Float64)',
#    'first_loss_date61':              'Nullable(Date)',
#    'first_loss_principle61':         'Nullable(Float64)',
#    'first_loss_interest61':          'Nullable(Float64)',
#    'first_loss_custsevicefee61':     'Nullable(Float64)',
#    'first_loss_insurancefee61':      'Nullable(Float64)',
#    'first_loss_vipcustsevicefee61':  'Nullable(Float64)',
#    'first_loss_date91':              'Nullable(Date)',
#    'first_loss_principle91':         'Nullable(Float64)',
#    'first_loss_interest91':          'Nullable(Float64)',
#    'first_loss_custsevicefee91':     'Nullable(Float64)',
#    'first_loss_insurancefee91':      'Nullable(Float64)',
#    'first_loss_vipcustsevicefee91':  'Nullable(Float64)',
#    'first_loss_date121':             'Nullable(Date)',
#    'first_loss_principle121':        'Nullable(Float64)',
#    'first_loss_interest121':         'Nullable(Float64)',
#    'first_loss_custsevicefee121':    'Nullable(Float64)',
#    'first_loss_insurancefee121':     'Nullable(Float64)',
#    'first_loss_vipcustsevicefee121': 'Nullable(Float64)',
#    'first_loss_date151':             'Nullable(Date)',
#    'first_loss_principle151':        'Nullable(Float64)',
#    'first_loss_interest151':         'Nullable(Float64)',
#    'first_loss_custsevicefee151':    'Nullable(Float64)',
#    'first_loss_insurancefee151':     'Nullable(Float64)',
#    'first_loss_vipcustsevicefee151': 'Nullable(Float64)',
#    'first_loss_date181':             'Nullable(Date)',
#    'first_loss_principle181':        'Nullable(Float64)',
#    'first_loss_interest181':         'Nullable(Float64)',
#    'first_loss_custsevicefee181':    'Nullable(Float64)',
#    'first_loss_insurancefee181':     'Nullable(Float64)',
#    'first_loss_vipcustsevicefee181': 'Nullable(Float64)',
#    'loan_row':                       'Nullable(UInt8)',
#    'data_date':                      'Nullable(Date)'}



















