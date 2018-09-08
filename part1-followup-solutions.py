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

[set pycharm environment]



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
    s3cnxn.get(s3files[idx],'data/'+s3files2[idx].key) # => download files

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
    ftpcnxn.get(s3files[0],'data/'+s3files2[idx].key) # => download files


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
df.compute()['date'].values[0]

# load most recent
cfg_files = list(glob.glob('data/vendorX/machinedata-*.csv'))
print(cfg_files)
df = pd.read_csv(np.sort(cfg_files[-1])) # => read most recent file
print(df.head())

# load all files

# dask fails
df = dd.read_csv('data/vendorX/machinedata-*.csv')
df.compute().head() # => fails

# solution 1: read with d6tstack
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'))
print(c.is_all_equal()) # => check if all equal before loading
print(c.is_col_present()) # => shows which columns are present
df = c.combine() # => load all files into pandas, all columns
df = c.combine(is_col_common=True) # => load all files into pandas, only common columns

# solution 2: use d6tstack to make aligned csv
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'), all_strings=True)
c.to_csv() # => creates csvs with columns matched
df = dd.read_csv('data/vendorX/machinedata-*-matched.csv')
df.compute().head() # => works
df.compute()['date'].values[0] # => dates not converted

def loadfile(dfg):
    dfg['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d')
    return dfg

# solution 3: use d6tstack to make combined parquet
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'), postprocessing=loadfile)
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
'''
mysql -u augvest -D augvest -p
LOAD DATA LOCAL INFILE 'data/vendorX/machinedata-2018-01.csv' INTO TABLE imported2 FIELDS TERMINATED BY ',' IGNORE 1 LINES;
select * from imported2 limit 2;
delete from imported2;
'''
df = pd.read_csv('data/vendorX/machinedata-2018-01.csv',nrows=5)
df['date'] = pd.to_datetime(df['date'], format='%Y-%m-%d') # => convert dates
print(pd.io.sql.get_schema(df, 'imported2').replace('"',"`"))

dfdb = pd.read_sql_table('imported2',sql_engine)
print(dfdb.head())
dfdb['date'].values[0]

# load multiple files
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'))
c.to_sql('mysql+mysqlconnector://augvest:augvest@localhost/augvest', 'imported3') # => creates csvs with columns matched
dfdb = pd.read_sql_table('imported3',sql_engine)
print(dfdb.head())
print(dfdb.columns)

'''
mysql -u augvest -D augvest -p
LOAD DATA LOCAL INFILE 'data/vendorX/machinedata-2018-01.csv' INTO TABLE imported4 FIELDS TERMINATED BY ',' IGNORE 1 LINES;
select * from imported2 limit 2;
delete from imported2;
'''

print(pd.io.sql.get_schema(df, 'imported2').replace('"',"`"))
c = d6tstack.combine_csv.CombinerCSV(glob.glob('data/vendorX/machinedata-*.csv'))
dft = c.preview_combine()
print(pd.io.sql.get_schema(dft, 'imported4').replace('"',"`")) # => todo: d6tstack postprocess

dfdb = pd.read_sql_table('imported4',sql_engine)

print(dfdb.head())
print(dfdb.columns)

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
