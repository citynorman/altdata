#****************************************
#****************************************
#****************************************
# qs
#****************************************
#****************************************
#****************************************
'''
timing:
20mins overview
30mins hands-on
15mins solutions overview

completed tutorial
GUI programs vs code
typical problems?
wish i had this:

completed sections => show results in real time

'''
#****************************************
#****************************************
#****************************************
# env setup
#****************************************
#****************************************
#****************************************
'''
virtualenv venv
source venv/bin/activate

pip install pandas d6tstack boto3 luigi requests dask[dataframe] mysql-connector-python

set up pycharm venv

'''

import glob
import os

def listvendorx():
    print(glob.glob('data/vendorX/*'))

def delvendorx():
    [os.remove(f) for f in glob.glob('data/vendorX/*')]
    listvendorx()

#****************************************
#****************************************
#****************************************
# get files
#****************************************
#****************************************
#****************************************

#****************************************
# s3
#****************************************

# CLI
'''
# aws configure

nano ~/.aws/credentials
[augvest]
AWS_ACCESS_KEY_ID = AKIAIM2OMJMEO7Y2OISA
AWS_SECRET_ACCESS_KEY = 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

# list files
aws s3 ls s3://test-augvest-20180719/vendorX/ --profile augvest

NOT:
aws s3 ls s3://test-augvest-20180719/vendorX
-- missing profile
-- missing /

# sync files
aws s3 sync s3://test-augvest-20180719/vendorX/ data/vendorX --profile augvest --dryrun
aws s3 sync s3://test-augvest-20180719/vendorX/ data/vendorX --profile augvest

'''

# luigi

delvendorx()

from luigi.contrib.s3 import S3Client
import ntpath
import os

cfg_key = 'AKIAIM2OMJMEO7Y2OISA'
cfg_secret = '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'
s3cnxn = S3Client(cfg_key, cfg_secret)
s3files = list(s3cnxn.listdir('s3://test-augvest-20180719/vendorX')) # => list files
s3files2 = list(s3cnxn.listdir('s3://test-augvest-20180719/vendorX',return_key=True))
print(s3files)
for idx, is3 in enumerate(s3files):
    ifname = 'data/'+s3files2[idx].key
    idir = ntpath.dirname(ifname)
    if not os.path.exists(idir):
        os.makedirs(idir)
    s3cnxn.get(s3files[idx],ifname) # => download files

listvendorx()

#****************************************
# ftp
#****************************************

# winscp

# python

delvendorx()

from ftpsync.targets import FsTarget
from ftpsync.ftp_target import FtpTarget
from ftpsync.synchronizers import DownloadSynchronizer

cfg_ftp_server = "104.131.61.25"
cfg_ftp_username = "testftpusr"
cfg_ftp_password = "WjNHUn5mjwQ3XMK"
cfg_ftp_path = "vendorX/"
ftpcnxn = FtpTarget("/"+cfg_ftp_path, cfg_ftp_server, username=cfg_ftp_username, password=cfg_ftp_password)
opts = {"force": True, "delete": False, "delete_unmatched": False, "verbose": 3}
s = DownloadSynchronizer(FsTarget("data/"+cfg_ftp_path), ftpcnxn, opts)
s.run()


# luigi
from luigi.contrib.ftp import RemoteFileSystem
import ntpath
import os

ftpcnxn = RemoteFileSystem(cfg_ftp_server, cfg_ftp_username, cfg_ftp_password)
ftpfiles = list(ftpcnxn.listdir('vendorX/')) # => list files
print(ftpfiles)
for idx, iftp in enumerate(ftpfiles):
    print(iftp)
    ifname = 'data/'+iftp
    idir = ntpath.dirname(ifname)
    if not os.path.exists(idir):
        os.makedirs(idir)
    ftpcnxn.get(ftpfiles[idx],ifname) # => download files


#****************************************
# api
#****************************************

import requests
import pandas as pd

cfg_api = {
  "token": "pWTXC2e-jqxr0hbWbUGQ-Q",
  "data": {
    "first_name": "nameFirst",
    "last_name": "nameLast",
      "_repeat": 100
  }
}

df_api = requests.post('http://app.fakejson.com/q', json = cfg_api)
df_api = pd.DataFrame(df_api.json()).head()


#****************************************
#****************************************
#****************************************
# loading data
#****************************************
#****************************************
#****************************************

#****************************************
# csv to pandas
#****************************************

import pandas as pd
import numpy as np
import glob
import dask.dataframe as dd
import d6tstack

# load one file
df = pd.read_csv('data/vendorX/machinedata-2018-01.csv')
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d') # => convert dates
print(df.head())
df['date'].values[0]

# load most recent
cfg_files = list(glob.glob('data/vendorX/machinedata-*.csv'))
print(cfg_files)
df = pd.read_csv(np.sort(cfg_files)[-1]) # => read most recent file
print(df.head())

# load all files

# dask fails
df = dd.read_csv('data/vendorX/machinedata-*.csv')
try:
    df.compute().head() # => fails
except:
    pass

# solution 1: read with d6tstack
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'))
print(c.is_all_equal()) # => check if all equal before loading
print(c.is_col_present()) # => shows which columns are present
df = c.combine() # => load all files into pandas, all columns
df = c.combine(is_col_common=True) # => load all files into pandas, only common columns

# solution 2: use d6tstack to make aligned csv
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'), all_strings=True)
c.to_csv() # => creates csvs with columns matched
df = dd.read_csv('data/vendorX/d6tstack-machinedata-*.csv')
df.compute().head() # => works
df.compute()['date'].values[0] # => dates not converted

def loadfile(dfg):
    dfg['date'] = pd.to_datetime(dfg['date'], format='%Y-%m-%d')
    return dfg

# solution 3: use d6tstack to make combined parquet
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'), apply_after_read=loadfile)
c.to_parquet(separate_files=False, out_filename='data/vendorX/combined.pq')
df = dd.read_parquet('data/vendorX/combined.pq')
df.compute().head() # => works
df.compute()['date'].values[0] # => dates converted

#****************************************
# csv to sql
#****************************************

# load single file
import sqlalchemy
sql_engine = sqlalchemy.create_engine('mysql+mysqlconnector://augvest:augvest@localhost/augvest')

df = pd.read_csv('data/vendorX/machinedata-2018-01.csv')
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d') # => convert dates
df.to_sql('imported',sql_engine,if_exists='replace',index=False)

dfdb = pd.read_sql_table('imported',sql_engine)
print(dfdb.head())
dfdb['date'].values[0]

# sql native

# => create table from pandas
df = pd.read_csv('data/vendorX/machinedata-2018-01.csv',nrows=5)
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d') # => convert dates
print(pd.io.sql.get_schema(df, 'imported2').replace('"',"`"))

# => mysql cli
'''
mysql -u augvest -D augvest -p
delete from imported2;
LOAD DATA LOCAL INFILE 'data/vendorX/machinedata-2018-01.csv' INTO TABLE imported2 FIELDS TERMINATED BY ',' IGNORE 1 LINES;
select * from imported2 limit 2;
'''

dfdb = pd.read_sql_table('imported2',sql_engine)
print(dfdb.head())
dfdb['date'].values[0]

# load multiple files - pandas

# => use d6tstack to directly write to db
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'))
c.to_sql('mysql+mysqlconnector://augvest:augvest@localhost/augvest', 'imported3') # => creates csvs with columns matched
dfdb = pd.read_sql_table('imported3',sql_engine)
print(dfdb.head())
print(dfdb.columns)

'''
mysql -u augvest -D augvest -p
delete from imported2;
LOAD DATA LOCAL INFILE 'data/vendorX/machinedata-2018-01.csv' INTO TABLE imported4 FIELDS TERMINATED BY ',' IGNORE 1 LINES;
select * from imported2 limit 2;
'''

# load multiple files - native db

# => use d6tstack to align files
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*0[0-9].csv'), apply_after_read=loadfile)
dft = c.preview_combine()

c2 = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'), all_strings=True, columns_select=dft.columns.tolist())
c2.to_csv(overwrite=True)

# => run sql commands
sql_create = pd.io.sql.get_schema(dft, 'imported4').replace('"',"`")
sql_engine.execute(sql_create)

# => load can only handle one file at a time
sql_engine.execute("delete from imported4;")
for ifile in np.sort(glob.glob('data/vendorX/d6tstack-machinedata-*.csv')):
    sql_load = "LOAD DATA LOCAL INFILE '%s' INTO TABLE imported4 FIELDS TERMINATED BY ',' IGNORE 1 LINES;" % ifile
    sql_engine.execute(sql_load)

dfdb = pd.read_sql_table('imported4',sql_engine)

print(dfdb.head())
print(dfdb.tail())
print(dfdb.columns)
dfdb['date'].values[0]


#****************************************
# excel to pandas
#****************************************
'''
# sync files
aws s3 sync s3://test-augvest-20180719/vendorY/ data/vendorY --profile augvest --dryrun
aws s3 sync s3://test-augvest-20180719/vendorY/ data/vendorY --profile augvest
'''

df = pd.read_excel('data/vendorY/xls-case-simple.xls')
print(df.head())

import d6tstack.convert_xls
from d6tstack.utils import PrintLogger
d6tstack.convert_xls.XLStoCSVMultiSheet('data/vendorY/xls-case-multisheet.xls').convert_all()
ddf = dd.read_csv('data/vendorY/xls-case-multisheet.xls-*.csv')
ddf.compute().head()

cfg_fnames = list(glob.glob('data/vendorY/xls-case-multifile*.xls'))
d6tstack.convert_xls.XLStoCSVMultiFile(cfg_fnames,cfg_xls_sheets_sel_mode='idx_global',cfg_xls_sheets_sel=0).convert_all()
ddf = dd.read_csv('data/vendorY/xls-case-multifile1.xls-*.csv')
ddf.compute().head()

d6tstack.utils.read_excel_advanced('data/vendorY/xls-case-badlayout1.xls',sheet_name='Sheet1', header_xls_range="B2:B2")
d6tstack.convert_xls.XLStoCSVMultiSheet('data/vendorY/xls-case-badlayout1.xls').convert_all(header_xls_range="B2:B2")
ddf = dd.read_csv('data/vendorY/xls-case-multisheet.xls-*.csv')
ddf.compute().head()


#****************************************
#****************************************
#****************************************
# d6tpipe preview
#****************************************
#****************************************
#****************************************




#****************************************
#****************************************
#****************************************
# preprocessing data
#****************************************
#****************************************
#****************************************

df = dd.read_parquet('data/vendorX/combined.pq')
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

