#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on Sun Aug 18 12:18:41 2019

@author: lee1984
'''

import setuptools

long_desc = 'Select ClickHouse data, convert to pandas dataframes and various other formats, ' + \
    'by using the ClickHouse HTTP interface'

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name = 'clickhouse2pandas-shicheng',
    version = '0.0.1',
    author = 'Shicheng Li',
    author_email = 'leegao36@163.com',
    description = 'Select ClickHouse data, convert to pandas dataframes',
    long_description = long_desc,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/lee19840806/clickhouse2pandas',
    packages = setuptools.find_packages(),
    classifiers = [
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent']
)
