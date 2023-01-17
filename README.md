# PySpark/Databricks UpSkilling POC 3



## Introduction
This POC is about creating PySpark application while using Git and CI/CD pipelines.
It takes data from csv files, filters it and saves result.


## Get started
This POC works on WSL.
Users run the program by running this example command:
```console 
user@LCE58202:/mnt/c/PATH/TO/PROJECT$ python3 main.py  --financial ./raw/financial.csv  --clients ./raw/clients.csv  --list Poland France
```
where:

* --financial argument is a path to a financial.csv file
* --clients argument is a path to clients.csv file
* --list argument is a list of countries to preserve

## Used dataset

### clients.csv
|name|description|type|
|--|--|--|
|id|unique identifier|int|
|first_name|client's first name|string|
|last_name|client's last name|string|
|email|client's email|string|
|gender|client's gender|string|
|country|client's home country|string|
|phone|client's phone|string|
|birthdate|client's birth date|date|

### financial.csv
|name|description|type|
|--|--|--|
|id|unique identifier|int|
|cc_t|credit's card type|string|
|cc_n|credit's card number|bigint|
|cc_mc|credit's card main currency|string|
|a|active flag|bool|
|ac_t|account type|string|

## Project components
This project contains:
* directories:
  * doc - directory where Sphinx doc are created
  * raw - directory where raw dataset files are stored
  * .github - folder with written GitHub Actions workflow
  * test - package with pytest tests written using Chispa package
  * client_data - directory where result csv is saved
* files:
  * functions.py
  * main.py
  * myapp.log
  * pyproject.toml
  * requirements.txt
