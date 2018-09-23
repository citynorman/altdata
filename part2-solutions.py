#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Sep  2 21:04:50 2018

@author: deepmind
"""
import numpy as np
import pandas as pd
import seaborn as sns

import fastparquet

#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1
# legal disclaimer: illustrative only, not to be used for investments
#!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!1

#****************************************
# d6tpipe preview
#****************************************

import d6tpipe.api
import d6tpipe.pipe

d6tapi = d6tpipe.api.APIClient()
pipe = d6tpipe.Pipe(d6tapi, 'riskminds2018-tutorial')
pipe.pull()
def loadpq(fname):
    return fastparquet.ParquetFile(pipe.files_one(fname)).to_pandas()
dfcass = loadpq('cass.pq')
dfwern = loadpq('werner-processed.pq')


#****************************************
# cass
#****************************************
dfcass = fastparquet.ParquetFile('data/financials/cass.pq').to_pandas()
    
def prepdf(dfg):
    dfg = dfg.reset_index()
    dfg['DATE_PUB'] = dfg['DATE']+pd.DateOffset(days=14)
    dfg['DATE_M'] = dfg['DATE'].dt.month
    dfg['DATE_YQ'] = (dfg['DATE'].dt.year).astype(str)+'Q'+(dfg['DATE'].dt.quarter).astype(str)
    return dfg

df1 = dfcass.pct_change()
df1 = prepdf(df1)

df12 = dfcass.pct_change(12)
df12 = prepdf(df12)

# analyze seasonality

df1.groupby(['DATE_M']).mean()
df12.groupby(['DATE_M']).mean()

sns.boxplot(x='DATE_M',y='FRGSHPUSM649NCIS',data=df1)
sns.boxplot(x='DATE_M',y='FRGSHPUSM649NCIS',data=df12)

#****************************************
# combined with werner
#****************************************

dfwern = fastparquet.ParquetFile('data/financials/werner-processed.pq').to_pandas()
df12m = df12.groupby('DATE_YQ').mean()

dfwern['DATE_YQ'] = (dfwern['date_fq'].dt.year).astype(str)+'Q'+(dfwern['date_fq'].dt.quarter).astype(str)
df_est_announce = dfwern.merge(df12m, on=['DATE_YQ'], how='left')
assert df_est_announce.isna().sum().sum()==0

df_est_announce2 = fastparquet.ParquetFile('data/financials/werner-cass.pq').to_pandas()
assert df_est_announce.equals(df_est_announce2)

#****************************************
#****************************************
# modeling
#****************************************
#****************************************

# correlate?
df_est_announce[['rev_yoy','rev_yoy_est','FRGSHPUSM649NCIS', 'FRGEXPUSM649NCIS']].corr()

df_est_announce.plot.scatter('FRGEXPUSM649NCIS','rev_yoy')
df_est_announce.plot.scatter('rev_yoy_est','rev_yoy')

# model
import sklearn.linear_model
import sklearn.metrics
from sklearn.metrics import mean_squared_error, r2_score, f1_score, accuracy_score


# simple model
m = sklearn.linear_model.LinearRegression()
r = m.fit(df_est_announce[['FRGEXPUSM649NCIS']], df_est_announce['rev_yoy'])
df_est_announce['rev_yoy_pred_ins'] = m.predict(df_est_announce[['FRGEXPUSM649NCIS']])

df_est_announce[['rev_yoy','rev_yoy_est','rev_yoy_pred_ins', 'FRGEXPUSM649NCIS']].corr()

df_est_announce['is_beat'] = df_est_announce['rev_yoy']>df_est_announce['rev_yoy_est']
df_est_announce['is_beat_pred'] = df_est_announce['rev_yoy_pred_ins']>df_est_announce['rev_yoy_est']

idxSel2010 = df_est_announce['date_announce'].dt.year>=2010

print(f1_score(df_est_announce['is_beat'],df_est_announce['is_beat_pred']))
print(accuracy_score(df_est_announce.loc[idxSel2010,'is_beat_pred'],df_est_announce.loc[idxSel2010,'is_beat']))
pd.crosstab(df_est_announce['is_beat'],df_est_announce['is_beat_pred'])

# out-sample
def run_outsample(dfg, m, Xcol, ycol):
    def predfun(dftrain,dftest):
        if dftrain.shape[0]<4: return np.nan
        r = m.fit(dftrain[Xcol], dftrain[ycol])
        if len(Xcol)==1:
            return m.predict(pd.DataFrame(dftest[Xcol]))[0]
        else:
            return m.predict(pd.DataFrame(dftest[Xcol]).T)[0]
        
    df_pred_ols = []
    for iper in range(0,df_est_announce.shape[0]):
        dftrain = df_est_announce.iloc[0:iper-1,:]
        dftest = df_est_announce.iloc[iper,:]
        pred = predfun(dftrain,dftest)
        df_pred_ols.append(pred)
    return df_pred_ols

mols = sklearn.linear_model.LinearRegression()
df_est_announce['rev_yoy_pred_os'] = run_outsample(df_est_announce, mols, ['FRGEXPUSM649NCIS'], 'rev_yoy')

df_est_announce[['rev_yoy','rev_yoy_est','rev_yoy_pred_os','rev_yoy_pred_ins', 'FRGEXPUSM649NCIS']].corr()
df_est_announce['is_beat_pred'] = df_est_announce['rev_yoy_pred_os']>df_est_announce['rev_yoy_est']
print(f1_score(df_est_announce['is_beat'],df_est_announce['is_beat_pred']))
print(accuracy_score(df_est_announce.loc[idxSel2010,'is_beat_pred'],df_est_announce.loc[idxSel2010,'is_beat']))
pd.crosstab(df_est_announce['is_beat'],df_est_announce['is_beat_pred'])


# add more variables?
from sklearn.model_selection import cross_validate
# in sample error go down?
-cross_validate(sklearn.linear_model.LinearRegression(), df_est_announce[['FRGEXPUSM649NCIS']].values, df_est_announce['rev_yoy'].values, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean()
-cross_validate(sklearn.linear_model.LinearRegression(), df_est_announce[['FRGSHPUSM649NCIS', 'FRGEXPUSM649NCIS']].values, df_est_announce['rev_yoy'].values, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean()

# other models
from sklearn.ensemble import AdaBoostRegressor
-cross_validate(AdaBoostRegressor(), df_est_announce[['FRGEXPUSM649NCIS']].values, df_est_announce['rev_yoy'].values, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean()
-cross_validate(AdaBoostRegressor(), df_est_announce[['FRGSHPUSM649NCIS', 'FRGEXPUSM649NCIS']].values, df_est_announce['rev_yoy'].values, return_train_score=False, scoring=('r2', 'neg_mean_squared_error'), cv=10)['test_neg_mean_squared_error'].mean()

df_est_announce['rev_yoy_predml_os'] = run_outsample(df_est_announce, AdaBoostRegressor(random_state=0), ['FRGSHPUSM649NCIS', 'FRGEXPUSM649NCIS'], 'rev_yoy')
df_est_announce['is_beat_predml'] = df_est_announce['rev_yoy_predml_os']>df_est_announce['rev_yoy_est']
print(f1_score(df_est_announce['is_beat_predml'],df_est_announce['is_beat']))
print(accuracy_score(df_est_announce['is_beat_predml'],df_est_announce['is_beat']))
print(accuracy_score(df_est_announce.loc[idxSel2010,'is_beat_predml'],df_est_announce.loc[idxSel2010,'is_beat']))
print(pd.crosstab(df_est_announce['is_beat_pred'],df_est_announce['is_beat'],normalize='columns',margins=True).round(2))
