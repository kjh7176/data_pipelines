# Data Pipelines
This is the fifth project of Udacity's **Data Engineering Nanodegree**:mortar_board:.  
The purpose is to build an **ETL pipeline** that extracts data from `S3`, stages them in `Redshift`, and transforms data into a set of fact and dimensional tables with `Apache Airflow`.

## Background
A music streaming company, :musical_note:*Sparkify*, has decided that it is time to introduce more automation and monitoring to their data warehouse ETL pipelines and come to the conclusion that the best tool to achieve this is `Apache Airflow`.  
The source data resides in S3 and needs to be processed in Sparkify's data warehouse in Amazon Redshift. The source datasets consist of JSON logs that tell about user activity in the application and JSON metadata about the songs the users listen to.

## File Description
- `dags/udac_example_dag.py` creats a dag consists of staging, loading and checking tasks.
- `plugins/helpers/create_tables.py` includes queries to create tables.
- `plugins/helpers/sql_queries.py` includes queries to select data for fact and dimension tables from staging tables.
- `plugins/operators/stage_redshift.py` defines a module `StageToRedshiftOperator` which stages data from s3 to a table in the redshift.
- `plugins/operators/load_fact.py` defines a module `LoadFactOperator` which loads data from staging table to fact table.
- `plugins/operators/load_dimension.py` defines a module `LoadDimensionOperator` which loads data from staging table to dimenstion table.
- `plugins/operators/data_quality.py` defines a module `DataQualityOperator` which checks data quality of each table.

## ETL pipeline in DAG
![dag](/images/dag.png)

## Examples of Data in Tables
> songplays
![songplays](/images/songplays.PNG)
   
> users
![users](/images/users.PNG)
   
> songs
![songs](/images/songs.PNG)

> artists
![artists](/images/artists.PNG)
  
> time
![time](/images/time.PNG)
