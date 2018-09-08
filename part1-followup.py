# -*- coding: utf-8 -*-


"""

http://tiny.cc/augvestnn

"""

import pandas as pd
pd.set_option('display.expand_frame_repr', False)

#****************************************
#****************************************
#****************************************
# get files
#****************************************
#****************************************
#****************************************

#****************************************
# get files - s3
#****************************************
cfg_key = 'AKIAIM2OMJMEO7Y2OISA'
cfg_secret = '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'


#****************************************
# get files - API
#****************************************
cfg_api_endpoint = 'https://reqres.in/api/users'

#****************************************
# get files - ftp
#****************************************
cfg_ftp_host = 'test.rebex.net'
cfg_ftp_user = 'demo'
cfg_ftp_pwd = 'password'
cfg_ftp_folder = '/pub/example' 


#****************************************
#****************************************
#****************************************
# ingest & load files
#****************************************
#****************************************
#****************************************

#****************************************
# csv
#****************************************

import pandas as pd
import numpy as np
# generate fake data
cfg_tickers = ['AAP','M','SPLS']
cfg_ntickers = len(cfg_tickers)
cfg_ndates = 10
cfg_dates = pd.bdate_range('2018-01-01',periods=cfg_ndates).tolist()+pd.bdate_range('2018-02-01',periods=cfg_ndates).tolist()
cfg_nobs = cfg_ndates*2
dft = pd.DataFrame({'date':np.tile(cfg_dates,cfg_ntickers), 'ticker':np.repeat(cfg_tickers,cfg_nobs)})

np.random.seed(0)
dft['data'] = np.random.normal(size=dft.shape[0])
dft['date_yyyymm'] = dft['date'].astype(str).str[:7]

cfg_dates2 = pd.bdate_range('2018-03-01',periods=cfg_ndates).tolist()+pd.bdate_range('2018-04-01',periods=cfg_ndates).tolist()
dft2 = pd.DataFrame({'date':np.tile(cfg_dates2,cfg_ntickers), 'ticker':np.repeat(cfg_tickers,cfg_nobs)})
dft2['data'] = np.random.normal(size=dft2.shape[0])
dft2['data_new'] = np.random.normal(size=dft2.shape[0])
dft2['date_yyyymm'] = dft2['date'].astype(str).str[:7]

for ig, dfg in dft.groupby('date_yyyymm'):
    dfg.to_csv('data-up/vendorX/machinedata-'+ig+'.csv',index=False)
for ig, dfg in dft2.groupby('date_yyyymm'):
    dfg.to_csv('data-up/vendorX/machinedata-'+ig+'.csv',index=False)


#****************************************
# xls
#****************************************
def write_file_xls(dfg, fname, sheets, startrow=0,startcol=0):
    writer = pd.ExcelWriter(fname)
    for isheet in sheets:
        dfg.to_excel(writer, isheet, index=False,startrow=startrow,startcol=startcol)
    writer.save()

# excel - bad case => d6tstack. Fake data
df = pd.DataFrame({'data':range(10)})
df = dft
write_file_xls(df, 'data-up/vendorY/xls-case-simple.xls',['Sheet1'])
write_file_xls(df, 'data-up/vendorY/xls-case-multisheet.xls',['Sheet1','Sheet2'])
write_file_xls(df, 'data-up/vendorY/xls-case-multifile1.xls',['Sheet1'])
write_file_xls(df, 'data-up/vendorY/xls-case-multifile2.xls',['Sheet1'])
write_file_xls(df, 'data-up/vendorY/xls-case-badlayout1.xls',['Sheet1','Sheet2'],startrow=1,startcol=1)




