#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep  4 11:22:12 2019

@author: lee1984
"""

import clickhouse2pandas
import unittest
import pandas

class TestInsertValidation(unittest.TestCase):
    def test_get_ch_data_range(self):
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
        data_range = clickhouse2pandas._get_ch_data_range(col_def)
        
        self.assertEqual(data_range.shape[0], 28)

if __name__ == '__main__':
    unittest.main()
