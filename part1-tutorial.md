# Hands-on Tutorial: Manipulating Alternative Data with Python, Pt 1

After the teachin, you can hands-on practice by completing this tutorial. You should be able to complete this using the reference piece and teach in code.

## Get Files

### S3

Dear client,
welcome to vendorX. We've set up your S3 bucket, the credentials are below.

key = AKIAIM2OMJMEO7Y2OISA  
secret = sRtWSf0jcvYmAW1RJt5mwJDPMKta5G9bqM8+rmI/  
bucket = s3://test-augvest-20180719  
path = vendorX/

Best,
vendorX team

#### Todo

* use S3browser to explore S3 bucket and sync to a local folder
* use AWS S3 CLI to explore S3 bucket and sync to a local folder

### API

Dear client,
welcome to vendorX. Below is our API endpoint.

API: http://app.fakejson.com/q  
Token: pWTXC2e-jqxr0hbWbUGQ-Q  
The documentation can be found at : https://fakejson.com/documentation  

Best,
vendorX team

#### Todo

* use Postman to POST to the api using the token
* use python requests to load 100 first name, last name into pandas


### FTP

Dear client,
welcome to vendorX. We've set up your ftp access, the credentials are below.

server = 104.131.61.25  
username = testftpusr  
password = WjNHUn5mjwQ3XMK  
path = vendorX/  

Best,
vendorX team

#### Todo

* use winscp to explore ftp server and sync to a local folder
* use pyftpsync to sync to a local folder


## Loading Data

VendorX data dictionary: 

Our data is in csv format. The descriptions are as follows

date: publication date  
ticker: NYSE exchange ticker  
data: some important alt data  
data_new: in March 2018 we added some other important alt data  


### CSV to pandas

* load `vendorX/machinedata-2018-01.csv` into pandas
* load all files from vendorX `vendorX/*.csv` into pandas using dask (HINT: use d6tstack)

### CSV to SQL

* load vendorX/machinedata-2018-01.csv into a sql database
	* use pandas
	* use db native command
* load `vendorX/*.csv` into a sql database (HINT: use d6tstack)
	* use pandas
	* use db native command


### xls to pandas

VendorY data dictionary: 

Our data is mostly available to fundamental analysts in Excel format. You can download a complete history of our Excel files from S3.

key = AKIAIM2OMJMEO7Y2OISA  
secret = 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  
path = vendorY/  

#### Todo

Get `vendorY` from S3 bucket above.

* load `vendorY/xls-case-simple.xls` into pandas
* load all sheets from `data/vendorY/xls-case-multisheet.xls` into pandas using dask 
* load all sheets from `data/vendorY/xls-case-badlayout1.xls` into pandas using dask 
* load all files `vendorY/xls-case-multifile*.xls` into pandas using dask

(HINT: use d6tstack)

