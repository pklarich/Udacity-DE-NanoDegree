import pandas as pd
import cassandra
import shutil
import os
import glob
import csv
from datetime import datetime
from sql_queries import insert_queries, check_queries        

def process_event_data_file(session):
    """
        process_event_data_file is a function that grabs data from event_datafile_new.csv,
        transforms the values to the proper data type, and loads them into their appropriate
        tables in the 'sparkify' keyspace in our Apache Cassandra environment. Data is read
        line by line and loaded to our defined tables using insert queries stored in 
        sql_queries.py.
        
        INPUTS:
        * session is the cassandra instance variable
    
    """

    # Open our event data file and read it line by line, uploading relevant data
    with open('event_datafile_new.csv', encoding = 'utf8') as f:
        rowtot = sum(1 for lines in f)
    with open('event_datafile_new.csv', encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # Skip header
        printcount = 0
        for i, line in enumerate(csvreader,1):
            session.execute(insert_queries['session_insert'], (line[0], int(line[3]),\
                            float(line[5]), int(line[8]), line[9]))
            session.execute(insert_queries['user_insert'], (line[0], line[9], line[1],\
                            int(line[3]), line[4], int(line[8]), int(line[10])))
            session.execute(insert_queries['song_insert'], (line[1], line[4], line[9], \
                            int(line[8]), int(line[3])))
            printcount += 1
            if printcount == 1000 or i == rowtot:
                print('{}/{} rows uploaded to keyspace'.format(i,rowtot), end = "\r", flush = True)
                printcount = 0
            
    print('Relevant data from event_datafile_new.csv has been uploaded to the sparkify keyspace. \n')
            
def process_data(session, folder_ext):
    """
        process_data is a function that walks through our defined folder_ext
        and creates a list of all of the filepaths that are contained within
        that folder. It then reads each lines from each file and appends it
        to a master data list (full_data_rows_list). These values are written
        to a new .csv file (event_datafile_new.csv) using a defined dialect
        (myDialect). 

        INPUTS:
        * session cassandra instance variable
        * folder_ext event_data folder path variable
    
    """

    # Create the filepath for the provided folder_ext
    
    filepath = os.getcwd() + '/event_data'
    print('{} is your root directory. \n'.format(filepath))
    
    # Gather all of the file paths for .csv files specifically
    # in our defined path
    
    for root, dirs, files in os.walk(filepath):
        # Looks like there is this folder hidden/created during
        # the project so this is instituted to ignore it 
        if ('.ipynb_checkpoints' in root):
            continue
        file_path_list = glob.glob(os.path.join(root,'*.csv'))   

    path_len = len(file_path_list)
    print('{} files found. \n'.format(path_len))
    
    # Create a list containing all of the data from the list
    # of files that were found
    
    full_data_rows_list = [] 
    for i, f in enumerate(file_path_list, 1): 
        with open(f, 'r', encoding = 'utf8', newline='') as csvfile: 
            csvreader = csv.reader(csvfile) 
            next(csvreader)
            for line in csvreader:
                full_data_rows_list.append(line)
    print('File data has been processed into a single list data type. \n')
    
    # If a data file already exists we will rename it with todays date and
    # place it into an archive folder. If this is run multiple times in a 
    # day the archived file will be overwritten.
    
    if os.path.exists('event_datafile_new.csv'):
        tday = datetime.now().strftime('%Y_%m_%d')
        os.makedirs('Archive',exist_ok = True)
        os.rename('event_datafile_new.csv','Archive/{}.csv'.format(tday))
        print('event_datafile_new.csv already exists and has been archived as {}.csv. \n'.format(tday))
        
   
    # Create a dialect to write a new csv file (event_datafile_new) with
    # making everything a string data type. The first row is a header row
    # and we only pull relevant columns from *full_data_rows_list*. If the
    # first row is empty it is know to not be relevant event data.
    
    csv.register_dialect('myDialect', quoting=csv.QUOTE_ALL, skipinitialspace=True) 
    
    with open('event_datafile_new.csv', 'w', encoding = 'utf8', newline='') as f:
        writer = csv.writer(f, dialect='myDialect')
        writer.writerow(['artist','firstName','gender','itemInSession','lastName',\
                        'length','level','location','sessionId','song','userId'])
        for i, row in enumerate(full_data_rows_list, 1):
            if (row[0] == ''): 
                continue
            writer.writerow((row[0], row[2], row[3], row[4], row[5], row[6], row[7],\
                             row[8], row[12], row[13], row[16]))  
        
    print('event_data has been processed and stored in event_datafile_new. \n')
        
def main():
    """
        main is the first function that runs and defines cluster
        and session within cassandra. It then runs a series of 
        functions to process data and store it within our keyspace.
        Once this has been completed the session and cluster are 
        shutdown.
    
    """

    from cassandra.cluster import Cluster
    cluster = Cluster()
    session = cluster.connect()


    # Set our keyspace to sparkify
    session.set_keyspace('sparkify')
    print('Connected to sparkify keyspace. \n')
    
    # Run the proper functions to ETL our data
    process_data(session, folder_ext='/event_data')
    process_event_data_file(session)
    
    
    # Shutdown our session and cluster
    session.shutdown()
    print('session is shutdown \n')
    cluster.shutdown()
    print('cluster is shutdown \n')
    print('Program \'etl\' has ended. \n')


if __name__ == "__main__":
    main()
