# Sparkify DB

The Sparkify database has been created to house information on users and their listening habits. 

## Pre-Requisites

An an S3 bucket and accompanying IAM role must be made prior to running the scripts. It is advised that the s3 bucket for output is location in the same region that the s3 data files are stored in (West-2). This information should be populated in the dl.cfg table. This can be accomplished via infrastructure as code using the boto3 library for python or via the AWS GUI.

A tutorial on cluster creation and related help can be found [here](https://docs.aws.amazon.com/redshift/latest/dg/tutorial-loading-data-launch-cluster.html)

It is necessary to have [Python](https://www.python.org/downloads/) installed along with the following libraries:

os
configparser
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, col
from pyspark.sql.functions import year, month, dayofmonth, hour, weekofyear, date_format, monotonically_increasing_id

Please refer to 
[here](https://docs.python.org/3/installing/index.html)
for further information on installing modules.

## Implementation

Running the etl.py will read, transform, and write the proper song and log data files to parquet files that are output to the s3 bucket. If any of these tables already exists it will overwrite them. Our tables are designed to be in a star schema. This can also be implement through the use of an EMR cluster if the user chooses to do so, utilizing an EMR cluster will make processing the files run more smoothly.

For more information on running Python scripts in a [batch file](https://datatofish.com/batch-python-script/)

## Database Design

The database is constructed in a star schema with the 'songplays' table as the Fact Table and the users, songs, artists, and time tables as the Dimension Tables. Data is extracted from .JSON files for both user logs and song files, placed into staging tables, transformed, and loaded into their respective tables. Queries can easily be written to access relevant data relating to songs and artists that individual users are listening to, at what time, in what location, and with what type of account. Tables designed with dissstyle ALL are designed to make querying quicker because of their small size. Other tables are designed with a distkey to best distribute the tables across nodes.

## Support

For support, complaints, recommendations, or tips please contact [Paul Klarich](pklarich@gmail.com) 