# Udacity Data Engineering With AWS - Data Warehouse Project

## Summary 
This project delivers a full ETL pipeline using infrastructure in AWS. It takes data from log files on songs and song information and stores them in '''staging songs''' and '''staging events''', respectively. It then transfers these into the tables '''songplays''', '''users''', '''songs''', '''artists''' and '''time'''. The data is then validated and checked for duplicates, ready for analytics. The star schema presents an elegant way to organise the data on AWS, with songplays, songs and time distributed by key and artists and users distrubuted by all. 

## Files
'''dwh.cfg''': 
- config file where relevant parameters related to the set up of the redshift cluster can be defined.

'''create_tables.py''': 
- Will check for iam role cluster and implement one if one does not already exist. 
- Will check for redshift cluster and implement one if one does not already exist.
- Will then create tables using queries listed in '''sql_queries.py'''. 

'''etl.py'''
- Will check for iam role cluster and implement one if one does not already exist. 
- Will check for redshift cluster and implement one if one does not already exist.
- Will transfer data from S3 bucket to staging tables according to relevant queries in '''sql_queries.py'''. 
- Will transfer data from staging tables to relevant tables. 

'''utilities.py'''
- Provides functions to create iam role, create a redshift cluster and delete a redshift cluster. 

'''validate.py'''
- checks the created tables for table size.
- will then also delete the cluster.

## Implementation
- Set relevant environment variables in the dwh.cfg file. 
- Run '''create_tables.py''' to drop and create relevant tables, connecting to AWS automatically via 
'''python3 create_tables.py'''
- Run '''etl.py''' to perform ETL process and populate tables via 
'''python3 etl.py'''
- Run '''validate.py''' to perform checks of the data via 
'''python3 validate.py'''
- For full process: 
''' python3 create_tables.py && python3 etl.py && python3 validate.py'''
