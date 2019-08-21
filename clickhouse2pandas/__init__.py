#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Aug 18 12:17:40 2019

@author: lee1984
"""

import urllib
import http
import json
import gzip
import time
import numpy
import pandas

name = 'clickhouse2pandas'
version = '0.0.3'

def select(connection_url, query = None, convert_to = 'DataFrame', settings = None):
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
        invalid_setting_keys = list(set(settings.keys()) - set(updated_settings.keys()))
        if len(invalid_setting_keys) > 0:
            raise ValueError('setting "{0}" is invalid, valid settings are: {1}'.format(
                invalid_setting_keys[0], ', '.join(updated_settings.keys())))

        updated_settings.update(settings)

    for i in updated_settings:
        updated_settings[i] = 1 if updated_settings[i] == True else 0 if updated_settings[i] == False else updated_settings[i]

    components = urllib.parse.urlparse(connection_url)

    if query is None:
        conn = http.client.HTTPConnection(components.hostname, port = components.port)
        conn.request('GET', '/')
        ret_value = conn.getresponse().read().decode().replace('\n', '')
    else:
        if query.strip(' \n\t').lower()[:6] not in ['select', 'descri']:
            raise ValueError('"query" should start with "select" or "describe", ' + \
                'while the provided "query" starts with "{0}"'.format(query.strip(' \n\t').split(' ')[0]))

    accepted_formats = ['DataFrame', 'TabSeparated', 'TabSeparatedRaw', 'TabSeparatedWithNames',
        'TabSeparatedWithNamesAndTypes', 'CSV', 'CSVWithNames', 'Values', 'Vertical', 'JSON', 'JSONCompact', 'JSONEachRow',
        'TSKV', 'Pretty', 'PrettyCompact', 'PrettyCompactMonoBlock', 'PrettyNoEscapes', 'PrettySpace', 'XML']

    if convert_to.lower() not in [i.lower() for i in accepted_formats]:
        raise ValueError('"convert_to" has an invalid value "{0}", it should be one of the following: {1}'.format(
            convert_to, ', '.join(accepted_formats)))

    clickhouse_format = 'JSON' if convert_to is None else 'JSONCompact' if convert_to.lower() == 'dataframe' else convert_to
    query_with_format = (query.rstrip('; \n\t') + ' format ' + clickhouse_format).replace('\n', ' ').strip(' ')

    http_get_params = {'user': components.username, 'password': components.password}
    http_get_params.update(updated_settings)
    conn = http.client.HTTPConnection(components.hostname, port = components.port)

    if updated_settings['enable_http_compression'] == 1:
        conn.request('POST', '/?' + urllib.parse.urlencode(http_get_params),
            body = gzip.compress(query_with_format.encode()),
            headers = {'Content-Encoding': 'gzip', 'Accept-Encoding': 'gzip'})
    else:
        conn.request('POST', '/?' + urllib.parse.urlencode(http_get_params), body = query_with_format.encode())

    resp = conn.getresponse()

    if resp.status == 404:
        error_message = gzip.decompress(resp.read()).decode() if updated_settings['enable_http_compression'] == 1 \
            else resp.read().decode()
        conn.close()
        raise ValueError(error_message)
    elif resp.status == 401:
        conn.close()
        raise ConnectionRefusedError(resp.reason + '. The username or password is incorrect.')
    else:
        if resp.status != 200:
            error_message = gzip.decompress(resp.read()).decode() if updated_settings['enable_http_compression'] == 1 \
                else resp.read().decode()
            conn.close()
            raise NotImplementedError('Unknown Error: status: {0}, reason: {1}, message: {2}'.format(
                resp.status, resp.reason, error_message))

    total = bytes()
    bytes_downloaded = 0
    last_time = time.time()

    while not resp.isclosed():
        bytes_downloaded += 300 * 1024
        total += resp.read(300 * 1024)
        if time.time() - last_time > 1:
            last_time = time.time()
            print('\rDownloaded: %.1f MB.' % (bytes_downloaded / 1024 / 1024), end = '\r')
    print()
    conn.close()

    ret_value = gzip.decompress(total).decode() if updated_settings['enable_http_compression'] == 1 else total.decode()

    if convert_to.lower() == 'dataframe':
        result_dict = json.loads(ret_value, strict = False)
        dataframe = pandas.DataFrame.from_records(result_dict['data'], columns = [i['name'] for i in result_dict['meta']])

        for i in result_dict['meta']:
            if i['type'] in ['DateTime', 'Nullable(DateTime)']:
                dataframe[i['name']] = pandas.to_datetime(dataframe[i['name']])

        ret_value = dataframe

    return ret_value

def insert(connection_url, db_table, df, settings):
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
        invalid_setting_keys = list(set(settings.keys()) - set(updated_settings.keys()))
        if len(invalid_setting_keys) > 0:
            raise ValueError('setting "{0}" is invalid, valid settings are: {1}'.format(
                invalid_setting_keys[0], ', '.join(updated_settings.keys())))

        updated_settings.update(settings)

    for i in updated_settings:
        updated_settings[i] = 1 if updated_settings[i] == True else 0 if updated_settings[i] == False else updated_settings[i]

    if db_table.find('.') < 1:
        raise ValueError('Argument "db_table" must be provided in the form of "your_db.your_table".')

    components = urllib.parse.urlparse(connection_url)

    describe_table = select(connection_url, 'describe table {0}'.format(db_table))

    non_nullable_colomns = list(describe_table[~describe_table['type'].str.startswith('Nullable')]['name'])
    integer_colomns = list(describe_table[describe_table['type'].str.contains('Int', regex = False)]['name'])
    missing_in_df = {i: numpy.where(df[i].isnull(), 1, 0).sum() for i in non_nullable_colomns}

    df_columns = list(df.columns)
    each_row = df.to_dict(orient = 'records')
    del df

    for i in missing_in_df:
        if missing_in_df[i] > 0:
            raise ValueError('"{0}" is not a nullable column, missing values are not allowed.'.format(i))

    for row in each_row:
        for col in df_columns:
            if pandas.isnull(row[col]):
                row[col] = None
            else:
                if col in integer_colomns:
                    try:
                        row[col] = int(row[col])
                    except:
                        raise ValueError('Column "{0}" is {1}, while value "{2}"'.format(col,
                            describe_table[describe_table['name'] == col].iloc[0]['type'], row[col]) + \
                            ' in the dataframe column cannot be converted to Integer.')

    json_each_row = '\n'.join([json.dumps(i, ensure_ascii = False) for i in each_row])
    del each_row

    query_with_format = 'insert into {0} format JSONEachRow \n{1}'.format(db_table, json_each_row)
    del json_each_row

    http_get_params = {'user': components.username, 'password': components.password}
    http_get_params.update(updated_settings)
    conn = http.client.HTTPConnection(components.hostname, port = components.port)

    if updated_settings['enable_http_compression'] == 1:
        conn.request('POST', '/?' + urllib.parse.urlencode(http_get_params),
            body = gzip.compress(query_with_format.encode()),
            headers = {'Content-Encoding': 'gzip', 'Accept-Encoding': 'gzip'})
    else:
        conn.request('POST', '/?' + urllib.parse.urlencode(http_get_params), body = query_with_format.encode())

    resp = conn.getresponse()

    if resp.status != 200:
        error_message = gzip.decompress(resp.read()).decode() if updated_settings['enable_http_compression'] == 1 \
            else resp.read().decode()
        conn.close()
        raise NotImplementedError('Unknown Error: status: {0}, reason: {1}, message: {2}'.format(
            resp.status, resp.reason, error_message))

    conn.close()
    print('Done.')
