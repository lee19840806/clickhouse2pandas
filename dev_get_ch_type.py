#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Sep  2 14:04:39 2019

@author: lee1984
"""

import sys

import numpy
import pandas

describe_table = [
    ['contract_id'                   , 'String'],
    ['objectno'                      , 'Nullable(String)'],
    ['user_id'                       , 'Nullable(String)'],
    ['cert_id'                       , 'Nullable(String)'],
    ['credit_ratif_date'             , 'Nullable(Date)'],
    ['putoutdate'                    , 'Nullable(Date)'],
    ['credit_amount'                 , 'Nullable(Float64)'],
    ['credit_term'                   , 'Nullable(UInt8)'],
    ['loan_term'                     , 'Nullable(UInt8)'],
    ['status'                        , 'Nullable(String)'],
    ['spu_id'                        , 'Nullable(String)'],
    ['sku_id'                        , 'Nullable(String)'],
    ['max_dpd'                       , 'Nullable(UInt16)'],
    ['max_dpd_3m'                    , 'Nullable(UInt16)'],
    ['max_dpd_month'                 , 'Nullable(UInt16)'],
    ['current_dpd'                   , 'Nullable(UInt16)'],
    ['first_due_date'                , 'Nullable(Date)'],
    ['last_repay_date'               , 'Nullable(Date)'],
    ['npd1'                          , 'Nullable(UInt8)'],
    ['npd3'                          , 'Nullable(UInt8)'],
    ['npd10'                         , 'Nullable(UInt8)'],
    ['npd30'                         , 'Nullable(UInt8)'],
    ['npd60'                         , 'Nullable(UInt8)'],
    ['npd90'                         , 'Nullable(UInt8)'],
    ['agr1'                          , 'Nullable(UInt8)'],
    ['agr3'                          , 'Nullable(UInt8)'],
    ['agr10'                         , 'Nullable(UInt8)'],
    ['agr30'                         , 'Nullable(UInt8)'],
    ['agr60'                         , 'Nullable(UInt8)'],
    ['agr90'                         , 'Nullable(UInt8)'],
    ['first_loss_date31'             , 'Nullable(Date)'],
    ['first_loss_principle31'        , 'Nullable(Float64)'],
    ['first_loss_interest31'         , 'Nullable(Float64)'],
    ['first_loss_custsevicefee31'    , 'Nullable(Float64)'],
    ['first_loss_insurancefee31'     , 'Nullable(Float64)'],
    ['first_loss_vipcustsevicefee31' , 'Nullable(Float64)'],
    ['first_loss_date61'             , 'Nullable(Date)'],
    ['first_loss_principle61'        , 'Nullable(Float64)'],
    ['first_loss_interest61'         , 'Nullable(Float64)'],
    ['first_loss_custsevicefee61'    , 'Nullable(Float64)'],
    ['first_loss_insurancefee61'     , 'Nullable(Float64)'],
    ['first_loss_vipcustsevicefee61' , 'Nullable(Float64)'],
    ['first_loss_date91'             , 'Nullable(Date)'],
    ['first_loss_principle91'        , 'Nullable(Float64)'],
    ['first_loss_interest91'         , 'Nullable(Float64)'],
    ['first_loss_custsevicefee91'    , 'Nullable(Float64)'],
    ['first_loss_insurancefee91'     , 'Nullable(Float64)'],
    ['first_loss_vipcustsevicefee91' , 'Nullable(Float64)'],
    ['first_loss_date121'            , 'Nullable(Date)'],
    ['first_loss_principle121'       , 'Nullable(Float64)'],
    ['first_loss_interest121'        , 'Nullable(Float64)'],
    ['first_loss_custsevicefee121'   , 'Nullable(Float64)'],
    ['first_loss_insurancefee121'    , 'Nullable(Float64)'],
    ['first_loss_vipcustsevicefee121', 'Nullable(Float64)'],
    ['first_loss_date151'            , 'Nullable(Date)'],
    ['first_loss_principle151'       , 'Nullable(Float64)'],
    ['first_loss_interest151'        , 'Nullable(Float64)'],
    ['first_loss_custsevicefee151'   , 'Nullable(Float64)'],
    ['first_loss_insurancefee151'    , 'Nullable(Float64)'],
    ['first_loss_vipcustsevicefee151', 'Nullable(Float64)'],
    ['first_loss_date181'            , 'Nullable(Date)'],
    ['first_loss_principle181'       , 'Nullable(Float64)'],
    ['first_loss_interest181'        , 'Nullable(Float64)'],
    ['first_loss_custsevicefee181'   , 'Nullable(Float64)'],
    ['first_loss_insurancefee181'    , 'Nullable(Float64)'],
    ['first_loss_vipcustsevicefee181', 'Nullable(Float64)'],
    ['loan_row'                      , 'Nullable(UInt8)'],
    ['data_date'                     , 'Nullable(Date)']]

describe_table = [
    ['column01', 'Int8'],
    ['column02', 'Int16'],
    ['column03', 'Int32'],
    ['column04', 'Int64'],
    ['column05', 'UInt8'],
    ['column06', 'UInt16'],
    ['column07', 'UInt32'],
    ['column08', 'UInt64'],
    ['column09', 'Float32'],
    ['column10', 'Float64'],
    ['column11', 'String'],
    ['column12', 'FixedString(18)'],
    ['column13', 'Date'],
    ['column14', 'DateTime'],
    ['column15', 'Nullable(Int8)'],
    ['column16', 'Nullable(Int16)'],
    ['column17', 'Nullable(Int32)'],
    ['column18', 'Nullable(Int64)'],
    ['column19', 'Nullable(UInt8)'],
    ['column20', 'Nullable(UInt16)'],
    ['column21', 'Nullable(UInt32)'],
    ['column22', 'Nullable(UInt64)'],
    ['column23', 'Nullable(Float32)'],
    ['column24', 'Nullable(Float64)'],
    ['column25', 'Nullable(String)'],
    ['column26', 'Nullable(FixedString(18))'],
    ['column27', 'Nullable(Date)'],
    ['column28', 'Nullable(DateTime)']]

col_def = pandas.DataFrame.from_records(describe_table, columns = ['name', 'type'])

#%% function body
col_def['is_nullable'] = col_def['type'].str.startswith('Nullable(')

col_def['range'] = numpy.where(col_def['is_nullable'], col_def['type'].str[9:-1], col_def['type'])

min_dict = {
    'Int8'  : -128,
    'Int16' : -32768,
    'Int32' : -2147483648,
    'Int64' : -9223372036854775808,
    'UInt8' : 0,
    'UInt16': 0,
    'UInt32': 0,
    'UInt64': 0}

max_dict = {
    'Int8'  : 127,
    'Int16' : 32767,
    'Int32' : 2147483647,
    'Int64' : 9223372036854775807,
    'UInt8' : 255,
    'UInt16': 65535,
    'UInt32': 4294967295,
    'UInt64': 18446744073709551615}

col_def['column_type'] = \
    numpy.where(col_def['range'].isin(list(min_dict.keys()) + ['Float32', 'Float64']), 'Numeric', 
    numpy.where(col_def['range'].str.startswith('FixedString'), 'FixedString', col_def['range']))

col_def['min'] = col_def['range'].apply(lambda x: min_dict.get(x, None))
col_def['max'] = col_def['range'].apply(lambda x: max_dict.get(x, None))

col_def['string_len'] = numpy.where(col_def['column_type'] == 'FixedString', col_def['range'].str[12:-1], None)
col_def['string_len'] = col_def['string_len'].astype(numpy.float64)









