# Hands-on Tutorial: Manipulating Alternative Data with Python, Pt 2

## Tutorial overview

This tutorials covers steps 4-5 of the data science process. Just as a reminder:
* Step 4 Preprocess data: clean, filter and transform data for it to be ready for modeling
* Step 5 Modeling: apply your analysis, make predictions and draw conclusions


## VendorX data

If you've followed [tutorial steps 1-3](https://github.com/citynorman/altdata/blob/master/part1-tutorial.md), you have obtained and loaded all files from vendorX. You can start preprocessing data for analysis. We will be using pandas to do this.

FYI: In case you haven't finished tutorial part 1, use [this file](https://s3-us-west-2.amazonaws.com/datasci-finance/data/vendorX-combined.pq) and load into pandas using dask `df=dd.read_parquet('vendorX-combined.pq').compute()`.

### Step 4: Preprocess Data

Basic:
* filter dataframe to only have values for ticker M
* filter dataframe to only have values for tickers M and SPLS without using OR
* to merge with factset data, make factset tickers of format [ticker]-US

Intermediate:
* extract ticker from factset tickers (ie ticker without "-US") - there are at least 2 ways of doing this
* select the last value of every month for each ticker
* make a column which first uses "data" and then "data_new" once available
* to merge with quarterly financials data, add a column with quarter and year like 1Q18
* calculate quarterly average of "data"

Advanced:
* create quarterly quintiles of "data" for each ticker
* make a cumulative sum of data for each ticker for each quarter as of each "date"
* assume the data gets published at the end of the month with two week delay. Add a column "publish_date" that contains the publishing date

## Cass Freight Data

The previous tutorials used realistic but fake data. In this exercise we will be using [Cass Freight Data](https://www.cassinfo.com/transportation-expense-management/supply-chain-analysis/cass-freight-index.aspx) obtained from [FRED](https://fred.stlouisfed.org/series/FRGSHPUSM649NCIS) to predict sales for a trucking company [Werner Enterprises](http://investor.werner.com/investor-relations/default.aspx). Specifically our goal is to use this data to predict sales beats/misses.

## Step 4: Preprocess Data - Cass data

### Get data
* [Cass raw](https://s3-us-west-2.amazonaws.com/datasci-finance/data/cass.pq)  
* [Werner financials](https://s3-us-west-2.amazonaws.com/datasci-finance/data/werner-processed.pq)  

```
import fastparquet
dfcass = fastparquet.ParquetFile('cass.pq').to_pandas()
```

### d6tpipe preview

[d6tpipe](https://github.com/d6tdev/d6tpipe) will make it easier to exchange data

```
import d6tpipe.api
import d6tpipe.pipe

d6tapi = d6tpipe.api.APIClient(key='9PCCZP5q9eN9abvm',secret='MLpTftafuH3bRAfX')
pipe = d6tpipe.Pipe(d6tapi, 'riskminds2018-tutorial')
pipe.pull()
dfcass = fastparquet.ParquetFile(pipe.files_one('cass.pq')).to_pandas()
```


### Data description

* date_fq: fiscal quarter end date
* date_announce: quarterly announcement date
* rev_yoy: actual yoy sales growth
* rev_yoy_est: broker consensus yoy sales growth
* rev_isbeat: sales beat broker consensus?

### Todo:

basic:
* analyze seasonality of Cass raw data
* combine Cass data with quarterly financials

## Step 5: Model Data

FYI: if you haven't done Step 4, use the [Cass+Werner combined file](https://s3-us-west-2.amazonaws.com/datasci-finance/data/werner-cass.pq)

### Todo:

basic:
* correlate Cass data with sales growth
* regress y=sales growth with x=Cass growth and assess accuracy of correctly predicting sales beats/misses

intermediate:
* make rolling out-sample predictions
* investigate if using both Cass variables works better
* use a sklearn machine learning model to predict sales growth and compare predictive accuracy to OLS
