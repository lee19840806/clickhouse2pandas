#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Thu Aug 22 11:56:07 2019

@author: lee1984
"""

import clickhouse2pandas
import unittest

class TestMergeSettings(unittest.TestCase):
    def test_1_user_defined_setting(self):
        user_settings = {'wait_end_of_query': 1}
        merged = clickhouse2pandas._merge_settings(user_settings)
        expected = {
            'enable_http_compression'                :   1,
            'send_progress_in_http_headers'          :   0,
            'log_queries'                            :   1,
            'connect_timeout'                        :  10,
            'receive_timeout'                        : 300,
            'send_timeout'                           : 300,
            'output_format_json_quote_64bit_integers':   0,
            'wait_end_of_query'                      :   1}
        
        self.assertEqual(merged, expected)
    
    def test_invalid_setting(self):
        user_settings = {'unknown_setting': 1}
        
        with self.assertRaises(ValueError):
            clickhouse2pandas._merge_settings(user_settings)
    
    def test_bool_to_int(self):
        user_settings = {'enable_http_compression': False, 'send_progress_in_http_headers': True}
        merged = clickhouse2pandas._merge_settings(user_settings)
        
        self.assertEqual(merged['enable_http_compression'], 0)
        self.assertEqual(merged['send_progress_in_http_headers'], 1)
    
    def test_none(self):
        merged = clickhouse2pandas._merge_settings(None)
        expected = {
            'enable_http_compression'                :   1,
            'send_progress_in_http_headers'          :   0,
            'log_queries'                            :   1,
            'connect_timeout'                        :  10,
            'receive_timeout'                        : 300,
            'send_timeout'                           : 300,
            'output_format_json_quote_64bit_integers':   0,
            'wait_end_of_query'                      :   0}
        
        self.assertEqual(merged, expected)

if __name__ == '__main__':
    unittest.main()
