#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 18 12:17:40 2019

@author: lee1984
"""

import urllib
import json
import pandas
import gzip

def select_data(connection_url, query = None, convert_to = 'DataFrame', settings = None):
    updated_settings = {
        'enable_http_compression'                :   1,
        'send_progress_in_http_headers'          :   0,
        'log_queries'                            :   1,
        'connect_timeout'                        :  10,
        'receive_timeout'                        : 300,
        'send_timeout'                           : 300,
        'output_format_json_quote_64bit_integers':   0,
        'wait_end_of_query'                      :   0}
    
    if settings is not None:
        invalid_setting_keys = set(settings.keys()) - set(updated_settings.keys())
        if len(invalid_setting_keys) > 0:
            print('setting "{0}" is invalid, valid settings are: {1}'.format(
                list(invalid_setting_keys)[0], ', '.join(updated_settings.keys())))
        
        updated_settings.update(settings)
    
    for i in updated_settings:
        updated_settings[i] = 1 if updated_settings[i] == True else 0 if updated_settings[i] == False else updated_settings[i]
    
    settings_params = '&'.join(['{0}={1}'.format(i, updated_settings[i]) for i in updated_settings])
    
    if connection_url[:4].lower() != 'http':
        raise ValueError('"connection_url" should start with "http"')
    
    at_r_index = connection_url.rfind('@')
    if at_r_index < 0:
        raise ValueError('"connection_url" must be formatted as "http://user:password@host:port"' + \
                         ' or "https://user:password@host:port"')
    
    user     = connection_url[:at_r_index].split('://')[1].split(':')[0]
    password = connection_url[:at_r_index].split('://')[1].split(':')[1]
    request_url = '{0}://{1}'.format(connection_url[:at_r_index].split('://')[0], connection_url[at_r_index + 1 :].strip('/'))
    
    if query is None:
        resp = urllib.request.urlopen(urllib.request.Request(url = request_url))
        ret_value = resp.read().decode().replace('\n', '')
    else:
        if query.strip(' \n\t').lower().startswith('select') == False:
            raise ValueError('"query" should start with "select", while the provided "query" starts with "{0}"'.format(
                repr(query[0])))
        
        accepted_formats = ['DataFrame', 'TabSeparated', 'TabSeparatedRaw', 'TabSeparatedWithNames', 
            'TabSeparatedWithNamesAndTypes', 'CSV', 'CSVWithNames', 'Values', 'Vertical', 'JSON', 'JSONCompact', 
            'JSONEachRow', 'TSKV', 'Pretty', 'PrettyCompact', 'PrettyCompactMonoBlock', 'PrettyNoEscapes', 
            'PrettySpace', 'XML']
        
        if convert_to.lower() not in [i.lower() for i in accepted_formats]:
            raise ValueError('"convert_to" has an invalid value "{0}", it should be one of the following: {1}'.format(
                convert_to, ', '.join(accepted_formats)))
        
        clickhouse_format = 'JSON' if convert_to is None else 'JSONCompact' if convert_to.lower() == 'dataframe' else convert_to
        query_with_format = query.rstrip('; \n\t') + ' format ' + clickhouse_format
        request_url = request_url + '/?user={0}&password={1}&{2}'.format(user, password, settings_params)
        
        query_result = urllib.request.urlopen(urllib.request.Request(
            url = request_url, data = query_with_format.encode(), headers = {'Accept-Encoding': 'gzip'})).read()
        
        if updated_settings['enable_http_compression'] == 1:
            resp = urllib.request.urlopen(urllib.request.Request(url = request_url, 
                data = gzip.compress(query_with_format.encode()), 
                headers = {'Content-Encoding': 'gzip', 'Accept-Encoding': 'gzip'}))
            query_result = gzip.decompress(resp.read()).decode()
        else:
            resp = urllib.request.urlopen(urllib.request.Request(url = request_url, data = query_with_format.encode()))
            query_result = resp.read().decode()
        
        ret_value = query_result
        
        if convert_to.lower() == 'dataframe':
            result_dict = json.loads(query_result, strict = False)
            cols = [i['name'] for i in result_dict['meta']]
            dataframe = pandas.DataFrame.from_records(result_dict['data'], columns = cols)
            
            for i in result_dict['meta']:
                if i['type'] in ['DateTime', 'Nullable(DateTime)']:
                    dataframe[i['name']] = pandas.to_datetime(dataframe[i['name']])
            
            ret_value = dataframe
        
        return ret_value
