# Overview
Sparkify, a music streaming startup, uses a data warehouse to support analytic functions. However, their user base has grown rapidly and they want to move their data to a data lake to enable flexible storage and analytics.

The objective of this project is to build an ETL pipeline thats extracts user activity and song data from the app, which is stored in S3, processes it using Spark and loads it into a set of dimensional tables in S3 for the analytics team to use to gain insights into which songs their users are listening to. 

The steps followed to achieve the above stated objectives are:
1. Extract user activity and song data from S3 in a JSON logs directory.
2. Using Spark, process the data and transform it into a set of dimensional tables.
3. Load the data back into S3

## Files
The project contains three files.
- `README.md`
- `etl.py`
- `dl.cfg`

### README.md
Briefly explains the goal of the project, what each file contains and how to run the python scripts.

### etl.py
contains logic to load the data from S3 into Spark for processing and creating a set of dimensional tables and loading it back to S3. 

### dl.cfg
contains AWS Acess Key configurations

## How to execute the project 
### Pre-requisites:
* Set up AWS CLI
* Set up Access crendentials using AWS IAM
* EC2 Login Key-Pair
* Create an EMR Cluster
* Launch and SHH into the EMR Cluster

1. Fill in access credentials in `dl.cfg`
2. Execute `etl.py` 

