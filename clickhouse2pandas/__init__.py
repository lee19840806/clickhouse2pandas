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
import pandas

name = 'clickhouse2pandas'
version = '0.0.2'

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
        if query.strip(' \n\t').lower().startswith('select') == False:
            raise ValueError('"query" should start with "select", while the provided "query" starts with "{0}"'.format(
                query.strip(' \n\t').split(' ')[0]))

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
            body = gzip.compress(query_with_format.encode('utf-8')),
            headers = {'Content-Encoding': 'gzip', 'Accept-Encoding': 'gzip'})
    else:
        conn.request('POST', '/?' + urllib.parse.urlencode(http_get_params), body = query_with_format.encode('utf-8'))

    resp = conn.getresponse()

    if resp.status == 404:
        error_message = gzip.decompress(resp.read()).decode() if updated_settings['enable_http_compression'] == 1 \
            else resp.read().decode()
        raise ValueError(error_message)
    elif resp.status == 401:
        raise ConnectionRefusedError(resp.reason)
    else:
        if resp.status != 200:
            raise NotImplementedError('Unknown Error: status: {0}, reason: {1}, message: {2}'.format(
                resp.status, resp.reason, resp.read().decode()))

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

    ret_value = gzip.decompress(total).decode() if updated_settings['enable_http_compression'] == 1 else total.decode()

    if convert_to.lower() == 'dataframe':
        result_dict = json.loads(ret_value, strict = False)
        dataframe = pandas.DataFrame.from_records(result_dict['data'], columns = [i['name'] for i in result_dict['meta']])

        for i in result_dict['meta']:
            if i['type'] in ['DateTime', 'Nullable(DateTime)']:
                dataframe[i['name']] = pandas.to_datetime(dataframe[i['name']])

        ret_value = dataframe

    return ret_value
