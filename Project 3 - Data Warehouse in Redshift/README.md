# Sparkify DB

The Sparkify database has been created to house information on users and their listening habits. 

## Pre-Requisites

An Amazon RedShift cluster, proper security group, sub-cluster, and IAM role must be made prior to running the scripts. It is advised that the cluster and sub-cluster are located in the same region that the s3 data files are stored in (West-2). This information should be populated in the dwh.cfg table. This can be accomplished via infrastructure as code using the boto3 library for python or via the aws GUI.

A tutorial on cluster creation and related help can be found [here](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-loading-data-launch-cluster.html)

It is necessary to have [Python](https://www.python.org/downloads/) installed along with the following libraries:

configparser
psycopg2

Please refer to 
[here](https://docs.python.org/3/installing/index.html)
for further information on installing modules.

## Implementation

Running the create_tables.py will create the tables within the database, this includes the staging tables. If any of these tables already exists it will drop and re-create them. After successfully running the create_tables script the etl script should be ran. The etl script will start by copying the data located in the s3 bucket that has been defined in the dwh configuration file and placing it into the staging tables. After this occurs the data will then be selected and inserted into their respective tables. Our tables are designed to be in a star schema. Distribution styles and sort keys are defined to garner the best performance from this process.

For more information on running Python scripts in a [batch file](https://datatofish.com/batch-python-script/)

## Database Design

The database is constructed in a star schema with the 'songplays' table as the Fact Table and the users, songs, artists, and time tables as the Dimension Tables. Data is extracted from .JSON files for both user logs and song files, placed into staging tables, transformed, and loaded into their respective tables. Queries can easily be written to access relevant data relating to songs and artists that individual users are listening to, at what time, in what location, and with what type of account. Tables designed with dissstyle ALL are designed to make querying quicker because of their small size. Other tables are designed with a distkey to best distribute the tables across nodes.

## Support

For support, complaints, recommendations, or tips please contact [Paul Klarich](pklarich@gmail.com) 

