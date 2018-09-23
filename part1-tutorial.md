# Hands-on Tutorial: Manipulating Alternative Data with Python, Pt 1

You can get hands-on practice manipulating data with python by completing this tutorial. It covers realistic exercises for getting, ingesting and preprocessing datasets. You should be able to complete this using the [reference pieces](https://alternativedata.org/the-best-tools-to-analyze-alternative-data-parts-2-3-ingesting-and-loading-data/) and [teach in code](https://github.com/citynorman/augvest201807/blob/master/part1.py).

## Tutorial overview

This tutorial cover the basics as well as addressing common problems such as:
* dealing with vendor data schema changes
* extracting data from Excel files


## Step 1: Get Data

### S3

Dear client,
welcome to vendorX. We've set up your S3 bucket, the credentials are below.

key = AKIAIM2OMJMEO7Y2OISA  
secret = sRtWSf0jcvYmAW1RJt5mwJDPMKta5G9bqM8+rmI/  
bucket = s3://test-augvest-20180719  
path = vendorX

Best,
vendorX team

#### Todo

* use [S3browser](http://s3browser.com/) to explore S3 bucket and sync to a local folder [basic]
* use [AWS S3 CLI](https://docs.aws.amazon.com/cli/latest/userguide/using-s3-commands.html) to explore S3 bucket and sync to a local folder [basic]
	* create a new security profile
	* preview files before sync
* use [python luigi](https://github.com/spotify/luigi) to list files and sync to a local folder [intermediate]

### FTP

Dear client,
welcome to vendorX. We've set up your ftp access, the credentials are below.

server = 104.131.61.25  
username = testftpusr  
password = WjNHUn5mjwQ3XMK  
folder = vendorX  

Best,
vendorX team

#### Todo

* use [winscp](https://winscp.net/eng/docs/introduction) to explore ftp server and sync to a local folder [basic]
* use pyftpsync to sync to a local folder [basic]
* use python luigi to list files and sync to a local folder [intermediate]


### API

Dear client,
welcome to vendorX. We've set up your API access, the credentials are below.

API endpoint: http://app.fakejson.com/q  
Token: pWTXC2e-jqxr0hbWbUGQ-Q  
The documentation can be found at: https://fakejson.com/documentation  

Best,
vendorX team

#### Todo

* use [Postman](https://www.getpostman.com/) to POST to the api using the token [basic]
* use [python requests](http://docs.python-requests.org/en/master/) to load 100 first name, last name into pandas [intermediate]


## Step 2+3: Ingest and Load Data

VendorX data dictionary: 

Our data is in csv format. The descriptions are as follows

date: publication date  
ticker: NYSE exchange ticker  
data: some important alt data  
data_new: in March 2018 we added some other important alt data  


### CSV to pandas

* load `vendorX/machinedata-2018-01.csv` into pandas [basic]
	* process "date" column into a date
* load the most recent file into pandas without hardcoding the filename [basic]
* load all files from vendorX `vendorX/machinedata-*.csv` into pandas using dask (HINT: use [d6tstack](https://github.com/d6t/d6tstack)) [intermediate]
	* make sure you do `ddf.tail()` or `ddf.compute()` after `dd.read_csv()`

### CSV to SQL

* load `vendorX/machinedata-2018-01.csv` into a sql database
	* use pandas
	* use db native command eg [mysql load file](https://dev.mysql.com/doc/refman/8.0/en/load-data.html)
* load `vendorX/machinedata-*.csv` into a sql database (HINT: use [d6tstack](https://github.com/d6t/d6tstack)) [intermediate]
	* use pandas
	* use db native command
* write a luigi pipeline that imports all data into db [advanced]


### xls to pandas

VendorY data dictionary: 

Our data is mostly available to fundamental analysts in Excel format. You can download a complete history of our Excel files from S3.

key = AKIAIM2OMJMEO7Y2OISA  
secret = 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  
path = vendorY/  

#### Todo

Get `vendorY` from S3 bucket above.

* load `vendorY/xls-case-simple.xls` into pandas [basic]
* load all sheets from `data/vendorY/xls-case-multisheet.xls` into pandas using dask  [basic]
* load all sheets from `data/vendorY/xls-case-badlayout1.xls` into pandas [basic]
* load all files `vendorY/xls-case-multifile*.xls` into pandas using dask [basic]

(HINT: use [d6tstack](https://github.com/d6t/d6tstack))
