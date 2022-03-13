# Capstone Project - NBA Database

## Project Summary

The goal if this project was to combine data from the NBA API and active NBA contract data from [basketball-reference.com](https://www.basketball-reference.com/contracts/). This data is a starting point to being able to analyze active players performance relative to their current pay-rate. The data being loaded allows for many different forms of performance based statistics to be calculated.

The structure of the project is as follows;

* Step 1: Scope the Project and Gather Data
* Step 2: Explore and Assess the Data
* Step 3: Define the Data Model
* Step 4: Run ETL to Model the Data
* Step 5: Complete Project Write Up

## Pre-Requisites

An Amazon RedShift cluster, s3 bucket, proper security group, sub-cluster, and IAM role must be made prior to running the scripts. It is advised that the cluster and sub-cluster are located in the same region that the s3 data files are stored in (West-2). This information should be populated in the dwh.cfg table. This can be accomplished via infrastructure as code using the boto3 library for python or via the AWS GUI.

A tutorial on cluster creation and related help can be found [here](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-loading-data-launch-cluster.html)

It is necessary to have [Python](https://www.python.org/downloads/) installed along with the following libraries:

* configparser
* psycopg2
* pandas
* boto3
* s3fs
* requests
* csv
* io
* datetime
* time
* os
* nba_api

Please refer to 
[here](https://docs.python.org/3/installing/index.html)
for further information on installing modules.

## Step 1: Scope the Project and Gather Data

### Scope

A high-level look at this project entails:

graph TD;
	A[Gather Data]-->B[Load into S3];
	B-->C[Create necessary tables in Redshift];
	C-->D[Copy data into staging tables];
	D-->E[Perform quality check for missing data];
	E-->F[Insert data in data tables];
	F-->G[Perform quality check on data];

### Data Gathering

| Data Set | Data Type | Description|
|----------|-----------|------------|
| NBA data | API       | Data collected from the publicly accessible NBA API including player, team, game, and statistical values|
| Contract | CSV       | NBA player compensation information for active players|

Data being gathered contains a variety of information and will need to be loaded into s3 for storage purposes.

## Step 2: Explore and Assess the Data

Data was initially explored using pandas dataframes and exporting small sets to CSV files to easily read and understand what was contained within them. Smaller datasets where used to test the s3 upload process, as well as the ETL process. 

In order to clean-up the data some steps were taken during the API connection process, utilizing pandas to import dataframes into s3 csv gzip files in a format that would be easy to work with and understand. Ensuring names were all in similar case helped to keep data consistent across both sources.

## Step 3: Define the Data Model

The data model is centered around the 'games' table as the facts table. The dimension tables include team_stats, team, contracts, players, and player_stats. The schema is closer to a snowflake schema with the foreign keys associated between tables.


### Pipeline Mapping

1. Collect data from relevant sources
2. Perform preliminary clean-up
3. Load datasets into s3
4. Perform data-quality check on s3 files
5. Copy s3 data into staging tables
6. Insert data into data model tables
7. Perform data-quality check on data model

## Step 4: Run Pipelines to Model the Data

See etl file. Belwo is an example query that was ran which locates players that have shot with either a field goal percentage of greater than 50% or a 3 point field goal percentage of greateer than 40% in the most recent season and then filters by players being paid less than 10 million dollars. This is a simple example of how this data set could potentially be used to determine under valued players.

```
SELECT s1.player_id,
last_name,
first_name,
fg_percent,
fg3_percent,
salary
FROM(
SELECT 
p1.player_id,
CAST(sum(fg) AS float) / sum(fga) as fg_percent,
CAST(sum(fg3m) AS float) / sum(fg3a) as fg3_percent
FROM 
player_stats as p1
WHERE fg > 0
AND fga > 10
AND fg3m > 0
AND fg3a > 10
AND game_id > 21000000
GROUP BY player_id)
AS s1
JOIN contracts as s2
ON s1.player_id = s2.player_id
JOIN players as s3
ON s1.player_id = s3.player_id
WHERE (fg_percent > .50 OR fg3_percent >.40)
AND salary < 10000000;
```

Below are the first 5 (out of 61) results from this query and the full results can be seen in the results.csv

![Query Results](https://github.com/pklarich/Udacity-DE-NanoDegree/blob/master/Capstone/results.png?raw=true)

### Create the data model

![Data Model](https://github.com/pklarich/Udacity-DE-NanoDegree/blob/master/Capstone/DataModel.png?raw=true)

### Perform data quality checks

See data_quality file
### Data dictionary

![Data Dictionary](https://github.com/pklarich/Udacity-DE-NanoDegree/blob/master/Capstone/DataDictionary.png?raw=true)

## Step 5: Complete Project Write Up

What's the goal? What queries will you want to run? How would Spark or Airflow be incorporated? Why did you choose the model you chose?
Clearly state the rationale for the choice of tools and technologies for the project.

The goal of this database is to be able to quickly and easily be able to access mass amounts of nba statistics at a time to relate to player earnings and other relevant information. Filtering player statistics to be able to see overperfomring and underperforming players could be of interest. 

Both Spark and Airflow could be easily incorporated. Spark could be implemented to load the data as parquet files into s3 prior to loading into the cluster. Airflow would be the most beneficial tool to implement given the somewhat extensive data loading process. Airflow would help schedule the process based on when games occur throughout the year.

Propose how often the data should be updated and why.

This data should be updated on atleast a weekly basis during the NBA regular season to keep adding relevant game and stat information for a week's games. This could be suspended when the regular season is not going on anymore.

Due to the more complex pipeline of accessing the data the main tools used were AWS Redshift and s3 as well as Python. Specifically the nba_api python library. It is an incredibly useful tool to use when accessing the NBA's stats API.
Include a description of how you would approach the problem differently under the following scenarios

If the data was increased by 100x.
If the data was increased by 100x implementing EMR would be beneficial, as well as increasing the number of nodes in the cluster. If data was increased Spark would be majorly benefial as well.

If the pipelines were run on a daily basis by 7am.
If the data needed to be updated daily by 7am Apache Airflow would need to be implemented to scheduled the processes.

If the database needed to be accessed by 100+ people.
If the data needed to be accessed by 100+ people the database would be housed in RedShift with multiple access groups of varying restrictions.


## Support

For support, complaints, recommendations, or tips please contact [Paul Klarich](pklarich@gmail.com) 

