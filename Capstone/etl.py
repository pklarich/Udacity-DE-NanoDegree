import psycopg2
import configparser
from sql_queries import insert_table_queries, staging_copy_queries
import pandas as pd
import boto3
import s3fs
import logging
import requests
import csv
from io import BytesIO
from datetime import date
import time
import os
from nba_api.stats.static import players, teams 
from nba_api.stats.endpoints import leaguegamefinder as lgf
from nba_api.stats.endpoints import commonteamroster as ctr
from nba_api.stats.endpoints import boxscoretraditionalv2 as bstv

def copyto_s3():
    """
    Copys all of the updated files into s3. Reload player/team information incase any of it has changed.
    Checks the most recent game that has been copied into s3 and copies all newer game data into s3.
    """
    # Get team data as df
    teams_df = pd.DataFrame(teams.get_teams())
    teamids = teams_df['id'].unique().tolist()

    # Get player data as df
    players_df = pd.DataFrame(players.get_players())
    count = 0
    
    # Get team rosters
    team_roster_df = pd.DataFrame()
    for ids in teamids:
        df = ctr.CommonTeamRoster(team_id = ids).get_data_frames()[0]
        team_roster_df = team_roster_df.append(df)
        count+=1
        print(count)
        time.sleep(1)
        
    # Find most recent game
    s3 = boto3.resource('s3')
    bucket = s3.Bucket('capstone-nba-bucket')
    most_recent = 0
                
    for obj_sum in bucket.objects.filter(Prefix="playerstats/"):
        ext = str(obj_sum.key).split('/')[1]
        try:
            num = int(ext)
        except(ValueError):
            print(f'Encountered Value error on {ext}')
            num = 0
        if num>most_recent:
            most_recent = num
        else:
            continue
    last_game = most_recent
    
    # Get list of game data and team stats for the previous how ever many seasons as df (20?)
    td = date.today().strftime('%m/%d/%Y')
    team_stats_df = lgf.LeagueGameFinder(
            league_id_nullable = '00',
            date_from_nullable='08/01/2010', 
            date_to_nullable=td, 
            season_type_nullable = 'Regular Season'
    ).get_data_frames()[0]
    team_stats_df = team_stats_df[team_stats_df.TEAM_ID.isin(teamids)]



    games_df = team_stats_df[team_stats_df.MATCHUP.str.contains(r'@')]
    games_df = games_df[["SEASON_ID","GAME_ID","GAME_DATE","MATCHUP"]].drop_duplicates(subset = ["GAME_ID"])
    games_df['HOME_TEAM'] = [x[:3] for x in games_df['MATCHUP']]
    games_df['AWAY_TEAM'] = [x[-3:] for x in games_df['MATCHUP']]

    games = games_df["GAME_ID"].unique().tolist()

    # Get list of recent games that haven't been added to s3 yet
    game_ids = list()
    for game in games:
        raw = int(game)
        if raw>last_game:
            game_ids.append(game)
        else:
            continue
    game_ids_len = len(game_ids)

    #Upload df into s3                
    df_list = (teams_df, players_df, team_roster_df, team_stats_df, games_df)
    df_str = ('teams_df', 'players_df', 'team_roster_df', 'team_stats_df', 'games_df')

    s3 = boto3.client('s3')
    for count, df in enumerate(df_list):
        csv_buffer = BytesIO()
        dfstr = df_str[count]
        df.to_csv(csv_buffer, index = False, compression = 'gzip', mode='wb',encoding='UTF-8')
        csv_buffer.seek(0)
        s3.upload_fileobj(csv_buffer, 'capstone-nba-bucket',f'{dfstr}')

    #Upload contract data to s3
    s3 = boto3.resource('s3')
    s3.meta.client.upload_file('contracts.csv', 'capstone-nba-bucket', 'contracts')

    #Create error log
    logging.basicConfig(filename='playerstats.log', level=logging.DEBUG)

    #Systemically upload player stats game by game
    s3 = boto3.client('s3')
    for numb, ids in enumerate(game_ids, start = 1):
        try:
            df = bstv.BoxScoreTraditionalV2(game_id = ids).get_data_frames()[0]
        except ValueError as err:
            print(f'Decoding JSON has failed on game {ids}')
            logging.error(f'Decoding JSON has failed on game {ids} due to ValueError')
            continue
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index = False, compression = 'gzip', mode='wb',encoding='UTF-8')
        csv_buffer.seek(0)
        s3.upload_fileobj(csv_buffer, 'capstone-nba-bucket',f'playerstats/{ids}')
        print(f"{numb} out of {game_ids_len} ")
        time.sleep(2)

    
def copyto_staging(cur,conn):
    """
    Copies information from the s3 bucket into the staging tables.
    """ 
    
    for query in staging_copy_queries:
        cur.execute(query)
        conn.commit()
        
def insert_records(cur,conn):
    """
    Inserts data from the staging tables into the dimensional model.
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
        
def main():
    """
    Reads the configuration file, connects to the database as defined by the dwh.cfg file,
    runs the three functions included in the program, then closes the database connection.
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    copyto_s3()
    copyto_staging(cur, conn)
    insert_records(cur, conn)
    
    conn.close()
    
if __name__=="__main__":
    main()
