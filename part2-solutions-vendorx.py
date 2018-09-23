#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep 23 13:30:05 2018

@author: deepmind
"""

import dask.dataframe as dd
import pandas as pd
import numpy as np

#****************************************
#****************************************
#****************************************
# preprocessing data
#****************************************
#****************************************
#****************************************

df = dd.read_parquet('data/vendorX/vendorX-combined.pq')
df = df.compute()
df = df.sort_values(['date','ticker'])

#****************************************
# basic
#****************************************

#* filter dataframe to only have values for ticker M
df[df['ticker']=='M']
#* filter dataframe to only have values for tickers M and SPLS without using OR
df[df['ticker'].isin(['M','SPLS'])]
#* to merge with factset data, make factset tickers of format [ticker]-US
df['ticker_fds'] = df['ticker']+'-US'

#****************************************
# Intermediate
#****************************************

#* extract ticker from factset tickers (ie ticker without "-US") - there are at least 2 ways of doing this
df['ticker2'] = df['ticker_fds'].str.split('-').str[0]
assert df['ticker2'].equals(df['ticker'])
#* select the last value of every month for each ticker
df.sort_values(['date','ticker']).groupby(['date_yyyymm','ticker']).tail(1)
#* make a column which first uses "data" and then "data_new" once available
df['data2'] = np.where(df['data_new'].isna(),df['data'],df['data_new'])
#* to merge with quarterly financials data, add a column with quarter and year like 1Q18
df['fq'] = (df['date'].dt.year).astype(str).str[-2:]+'Q'+(df['date'].dt.quarter).astype(str)
#* calculate quarterly average of "data"
df.groupby(['fq'])['data'].mean()

#****************************************
# Advanced:
#****************************************

# * create quarterly quintiles of "data"
df['data_q'] = df.groupby(['ticker','fq'])['data'].transform(lambda x: pd.qcut(x,5,labels=range(5)))
#* make a cumulative sum of data for each ticker for each quarter as of each "date"
df['data_agg'] = df.groupby(['ticker','fq'])['data'].cumsum()
#* assume the data gets published at the end of the month with two week delay. Add a column "publish_date" that contains the publishing date
df['publish_date'] = df['date'] + pd.tseries.offsets.MonthEnd() + pd.DateOffset(days=14)