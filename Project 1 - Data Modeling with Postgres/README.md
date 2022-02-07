# Sparkify DB

The Sparkify database has been created to house information on users and their listening habits. 

## Pre-Requisites

It is necessary to have [Python](https://www.python.org/downloads/) installed along with the following libraries:

os
glob
psycopg2
pandas

Please refer to 
[here](https://docs.python.org/3/installing/index.html)
for further information on installing modules.

## Implementation

Running the create_tables.py will create the tables within the database. Using PRIMARY KEYS and UPSERT commands allow for us to only need to run this file once to establish the data tables. Any time data needs to be updated the etl.py file can be ran on its own. These files can housed within a batch file and set to run on a weekly, monthly, or quarterly basis, per user preference.

For more information on running Python scripts in a [batch file](https://datatofish.com/batch-python-script/)

## Database Design

The database is constructed in a star schema with the 'songplays' table as the Fact Table and the users, songs, artists, and time tables as the Dimension Tables. Data is extracted from .JSON files for both user logs and song files, transformed, and loaded into their respective tables. Queries can easily be written to access relevant data relating to songs and artists that individual users are listening to, at what time, in what location, and with what type of account. 

## Support

For support, complaints, recommendations, or tips please contact [Paul Klarich](pklarich@gmail.com) 