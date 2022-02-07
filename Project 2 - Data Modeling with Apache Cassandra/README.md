# Sparkify Keyspace

The Sparkify keyspace has been created to house information on users and their listening habits. 

## Pre-Requisites

It is necessary to have [Python](https://www.python.org/downloads/) installed along with the following libraries:

pandas
cassandra
shutil
os
glob
csv
datetime

Please refer to 
[here](https://docs.python.org/3/installing/index.html)
for further information on installing modules.

## Implementation

Running the create_tables.py will create the tables within the keyspace. This program should only be run to build our tables in the sparkify keyspace. If the tables in the keyspace need to be wiped, the create_tables program can be run. Any time data needs to be updated the etl.py file can be ran on its own. To check our queries that the tables are based around, the check_tables program can be ran. These files can housed within a batch file and set to run on a weekly, monthly, or quarterly basis, per user preference.

For more information on running Python scripts in a [batch file](https://datatofish.com/batch-python-script/)

## Database Design

The database is constructed in Apache Cassandra and is a NoSQL database, or Non-Relational. Three tables are built to meet the requirement of the queries supplied. These queries are:

1. Give me the artist, song title and song's length in the music app history that was heard during sessionId = 338, and itemInSession = 4

2. Give me only the following: name of artist, song (sorted by itemInSession) and user (first and last name) for userid = 10, sessionid = 182

3. Give me every user name (first and last) in my music app history who listene to the song 'All Hands Against His Own'

## Support

For support, complaints, recommendations, or tips please contact [Paul Klarich](pklarich@gmail.com) 