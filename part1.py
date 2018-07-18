# -*- coding: utf-8 -*-
"""
Spyder Editor

This is a temporary script file.
"""

import pandas as pd
pd.set_option('display.expand_frame_repr', False)

#****************************************
#****************************************
#****************************************
# prep
#****************************************
#****************************************
#****************************************

# todo: jupyter notebook for d6tstack xls loading

'''
CREATE DATABASE augvest;
CREATE USER 'augvest'@'localhost' IDENTIFIED BY 'augvest';
GRANT ALL PRIVILEGES ON * . * TO 'augvest'@'localhost';
FLUSH PRIVILEGES;

'''

#****************************************
#****************************************
#****************************************
# survey
#****************************************
#****************************************
#****************************************
# sli.do/augvest201807

#****************************************
#****************************************
#****************************************
# environment setup
#****************************************
#****************************************
#****************************************

# motvation + goal:

'''

anaconda
=> spyder vs jupyter notebook

environments?
virtualenv
conda env
vagrant



https://docs.aws.amazon.com/cli/latest/userguide/installing.html
https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-getting-started.html

'''
#****************************************
#****************************************
#****************************************
# get files
#****************************************
#****************************************
#****************************************

# goal+intro:  see altdata.org

# data prep / data integration software

#****************************************
# get files - s3
#****************************************
cfg_key = 'AKIAIM2OMJMEO7Y2OISA'
cfg_secret = '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'

'''
GUI S3 Browser
 
Tools\Folder Sync tool

CLI
aws s3 ls s3://test-augvest-20180719/
aws s3 cp s3://test-augvest-20180719/s3-201807/a.csv  C:\dev\augvest201807\data\s3sync\a.csv
aws s3 sync s3://test-augvest-20180719/  C:\dev\augvest201807\data\s3sync\

'''

import boto3
session = boto3.Session(
    aws_access_key_id=cfg_key,
    aws_secret_access_key=cfg_secret,
)
s3 = session.resource('s3')
s3cnxn = s3.Bucket('test-augvest-20180719')

for object in s3cnxn.objects.all():
    print(object)
    
s3cnxn.download_file('s3-201807/a.csv', 'data/s3sync/a.csv')
    
#****************************************
# get files - API
#****************************************
'''
GUI Postman

'''

import requests
import json

r = requests.get('https://reqres.in/api/users')
r.json()['data']

# auth
r = requests.get('https://api.github.com/user', auth=('user@company.com', 'password'))
r = requests.get('https://reqres.in/api/users', headers={"Authorization":"Bearer MYREALLYLONGTOKENIGOT"})
# todo: example with auth key from mscience
# NB: IP restrictions

#****************************************
# get files - ftp
#****************************************
# GUI - winscp (download)
# todo: winscp proxy auth?

# programmatic

import ftputil
import os

ftpcnxn = ftputil.FTPHost('test.rebex.net', 'demo', 'password')
ftpcnxn.listdir('/')
ftpcnxn.listdir('/pub/example')

for dir_, _, files in ftpcnxn.walk('/'):
    for fileName in files:
        relDir = os.path.relpath(dir_, '/')
        relFile = os.path.join(relDir, fileName)
        print(relFile)

ftpcnxn.close()


#****************************************
# proxy
#****************************************

# environmental variables
# works for: pip, aws, [requests]

# todo: cntlm command

#****************************************
# regular updates
#****************************************
# s3
'''
s3 cli: => bash script
aws s3 sync s3://test-augvest-20180719/  C:\dev\augvest201807\data\s3sync\
'''

# http
# write own logic
# todo: example with date keys
r = requests.get('https://reqres.in/api/users', data={'date_start':'2018-07-19', 'tickers':['AAPL','MSFT']})

# ftp
'''
winscp batch
pyftpsync

'''

# huey regular tasks

# circus process management

#****************************************
#****************************************
#****************************************
# ingest & load files
#****************************************
#****************************************
#****************************************

# goal+intro:  see altdata.org

#****************************************
# csv
#****************************************

# csv, txt - easy case
import pandas as pd
import glob
import d6tstack.combine_csv as d6tc

# generate fake data
dft=pd.DataFrame({'data':range(10)})
print(dft)
dft.to_csv('data/s3-201806/a.csv',index=False)
pd.DataFrame({'data':range(10),'data2':range(10)}).to_csv('data/s3-201807/b.csv',index=False)
dft.to_csv('data/s3-201807/a.csv',index=False)

# read single file
df = pd.read_csv('data/s3-201806/a.csv')
df

# read multiple files
df = pd.read_csv('data/s3-20180*/a.csv')

# read multiple files
import dask.dataframe as dd
ddf = dd.read_csv('data/s3-2018*/a.csv')
ddf.compute()

# what's going on?
ddf = dd.read_csv('data/*/a.csv')
ddf.compute()

cfg_fnames = list(glob.glob('data/*/a.csv'))
c = d6tc.CombinerCSV(cfg_fnames, all_strings=True) # all_strings=True makes reading faster
c.is_all_equal()
c.is_col_present().reset_index(drop=True)
cfg_fnames

# csv, txt - d6stack dask schema change problem

# csv, txt - d6stack vendor example

# csv, txt - d6stack additional workbook

# reading gzip files
# todo: example with s3 gz data


#****************************************
# xls
#****************************************

# excel - good case
def write_file_xls(dfg, fname, sheets, startrow=0,startcol=0):
    writer = pd.ExcelWriter(fname)
    for isheet in sheets:
        dfg.to_excel(writer, isheet, index=False,startrow=startrow,startcol=startcol)
    writer.save()

write_file_xls(df, 'data/xls-case-simple.xls',['Sheet1'])
pd.read_excel('data/xls-case-simple.xls',sheet_name='Sheet1')

# excel - bad case => d6tstack. Fake data
write_file_xls(df, 'data/xls-case-multisheet.xls',['Sheet1','Sheet2'])

import d6tstack.convert_xls
from d6tstack.utils import PrintLogger
d6tstack.convert_xls.XLStoCSVMultiSheet('data/xls-case-multisheet.xls').convert_all()
ddf = dd.read_csv('data/xls-case-multisheet.xls-*.csv')
ddf.compute()

write_file_xls(df, 'data/xls-case-multifile1.xls',['Sheet1'])
write_file_xls(df, 'data/xls-case-multifile2.xls',['Sheet1'])

cfg_fnames = list(glob.glob('data/xls-case-multifile*.xls'))
d6tstack.convert_xls.XLStoCSVMultiFile(cfg_fnames,cfg_xls_sheets_sel_mode='idx_global',cfg_xls_sheets_sel=0).convert_all()
ddf = dd.read_csv('data/xls-case-multifile1.xls-*.csv')
ddf.compute()

write_file_xls(df, 'data/xls-case-badlayout1.xls',['Sheet1','Sheet2'],startrow=1,startcol=1)
d6tstack.utils.read_excel_advanced('data/xls-case-badlayout1.xls',sheet_name='Sheet1', header_xls_range="B2:B2")
d6tstack.convert_xls.XLStoCSVMultiSheet('data/xls-case-badlayout1.xls').convert_all(header_xls_range="B2:B2")
ddf = dd.read_csv('data/xls-case-multisheet.xls-*.csv')
ddf.compute()


# excel - bad case => d6tstack. vendor-credit

# excel - bad case => d6tstack. vendor-pos

# json
pd.DataFrame(r.json()['data'])

#****************************************
# sql
#****************************************
# import csv into db

# heidi sql => LMGTFY

# programmatic
df=pd.DataFrame({'data':range(10)})

import sqlalchemy
sql_engine = sqlalchemy.create_engine('mysql+mysqlconnector://augvest:augvest@localhost/augvest')
# todo: create user with priviledges. put in prep section

# db commands
'''
mysql -u augvest -p augvest
LOAD DATA INFILE '' INTO TABLE imported;

'''
# import a.csv
pd.read_sql_table('imported',sql_engine)
# import b.csv => fix with d6tstack

# pandas to sql
df.to_sql('a',sql_engine,if_exists='replace',index=False)
pd.read_sql_table('a',sql_engine)
df.to_sql('a',sql_engine,if_exists='append',index=False)
pd.read_sql_table('a',sql_engine)

# dask NO to sql

# odo import to db
import odo
odo.odo('data/s3-201806/a.csv', 'mysql+mysqlconnector://augvest:augvest@localhost/augvest::imported')
pd.read_sql_table('imported',sql_engine)

# excel? convert to csv using d6stack

#****************************************
# regular updates
#****************************************

# csv, txt, excel 
# option 1: rerun with more data
# option 2: incremental run


# database
dfts1 = pd.DataFrame({'date':pd.date_range('2018-01-01',periods=5), 'data':range(5)})
dfts1.to_sql('ts',sql_engine,if_exists='replace',index=False)
pd.read_sql_table('ts',sql_engine)

# option 1: rewrite
dfts2 = pd.DataFrame({'date':pd.date_range('2018-01-01',periods=6), 'data':range(6)})
dfts2.to_sql('ts',sql_engine,if_exists='replace',index=False)
pd.read_sql_table('ts',sql_engine)

# option 2: append
dfts1.to_sql('ts',sql_engine,if_exists='replace',index=False)
dftsdbmax = pd.read_sql_query('select max(date) from ts',sql_engine)
dfts2[dfts2['date']>dftsdbmax['max(date)'].max()].to_sql('ts',sql_engine,if_exists='append',index=False)
pd.read_sql_table('ts',sql_engine)


#****************************************
#****************************************
#****************************************
# pt2 preview
#****************************************
#****************************************
#****************************************

# d6tjoin

