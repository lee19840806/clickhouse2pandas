#!/usr/bin/env python3
# -*- coding: utf-8 -*-
'''
Created on Sun Aug 18 12:18:41 2019

@author: lee1984
'''

import setuptools

with open('README.md', 'r') as fh:
    long_description = fh.read()

setuptools.setup(
    name = 'clickhouse2pandas',
    version = '0.0.3',
    author = 'Shicheng Li',
    author_email = 'leegao36@163.com',
    description = 'Select ClickHouse data, convert to pandas dataframes',
    long_description = long_description,
    long_description_content_type = 'text/markdown',
    url = 'https://github.com/lee19840806/clickhouse2pandas',
    packages = setuptools.find_packages(),
    python_requires = '>=3',
    keywords = 'clickhouse pandas dataframe',
    project_urls = {
        'ClickHouse DB': 'https://clickhouse.yandex/',
        'Pandas': 'https://pandas.pydata.org/'},
    classifiers = [
        'Development Status :: 3 - Alpha',
        'Programming Language :: Python :: 3',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent'],
    install_requires = ['pandas>=0.20.0']
)
