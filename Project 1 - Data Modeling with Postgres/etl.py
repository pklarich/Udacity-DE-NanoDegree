import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    """
        process_song_file is a function that reads a .json file whos location
        is defined by *filepath*. The relevant artist information and stores it 
        within the *artists* data table.
        It then extracts the relevant song information is extracted 
        and stored within the *songs* data table in the Sparkify database.
        INPUTS:
        * cur the cursor variable
        * filepath the song file's filepath
    """
    
    # open song file
    df = pd.read_json(filepath, typ='series')
    
    # insert artist record
    artist_data = [df.values[1],df.values[5],df.values[4],df.values[2],df.values[3]]
    cur.execute(artist_table_insert, artist_data)

    # insert song record
    song_data = [df.values[6],df.values[7],df.values[1],df.values[9],df.values[8]]
    cur.execute(song_table_insert, song_data)


def process_log_file(cur, filepath):
    """
        process_log_file is a function that reads a .json file whos location
        is defined by *filepath*. The timestamp is extracted, transformed into
        various forms defined by column labels and stored within the *time* data table.
        It then extracts the relevant user information and stores it within the *users*
        data table. Finally the songplay data that is related to the users is extracted
        from both the log file and the *songs* and *artists* tables using a SQL query to 
        match song information based off of song name, artist name, and song duration. 
        this data is then inserted into the *songplays* table.
        INPUTS:
        * cur the cursor variable
        * filepath the log file's filepath
    """
    # open log file
    df = pd.read_json(filepath, lines = True)

    # filter by NextSong action
    df = df.loc[df['page'] == 'NextSong']
    df.ts = pd.to_datetime(df['ts'], unit = 'ms')

    # convert timestamp column to datetime
    t = df.ts
    
    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.weekofyear,\
             t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ('star_time', 'hour', 'day', 'week', 'month', 'year', 'weekday')
    time_df = pd.DataFrame(dict(zip(column_labels,time_data)))

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_df = df[['userId','firstName','lastName','gender','level']]
    user_df = user_df.drop_duplicates()

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = (row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent)
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    """
        process_data is a function that walks through our defined filepath
        and creates a list of all of the filepaths that are JSON files.
        The defined func will then run for each of these files. 
        
        INPUTS:
        * cur the cursor variable
        * conn the connection variable
        * filepath the root filepath
        * func the defined function our JSON files are processed by
    """
    
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
        main is the first function that runs and defines our conn
        variable with the connection to the Sparkify database. 
        It then defines the cur variable with out connection
        cursor. From there it runs the process_data function to 
        process both the songs files and the log files. After that
        is complete the connection is closed.
        
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()