import psycopg2
import configparser
from sql_queries import create_table_queries, drop_table_queries

def initial_copyto_s3():
    """
    Copys all of the initial data into s3.
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
        time.sleep(3)

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

    game_ids = games_df["GAME_ID"].unique().tolist()
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


    #Systemically upload player stats game by game
    s3 = boto3.client('s3')
    player_stats_df = pd.DataFrame()
    for numb, ids in enumerate(game_ids, start = 1):
        try:
            df = bstv.BoxScoreTraditionalV2(game_id = ids).get_data_frames()[0]
        except ValueError:
            print(f'Decoding JSON has failed on game {ids}')
            continue
        player_stats_df = player_stats_df.append(df)
        csv_buffer = BytesIO()
        df.to_csv(csv_buffer, index = False, compression = 'gzip', mode='wb',encoding='UTF-8')
        csv_buffer.seek(0)
        s3.upload_fileobj(csv_buffer, 'capstone-nba-bucket',f'playerstats/{ids}')
        print(f"{numb} out of {game_ids_len} ")
        time.sleep(3)

def drop_tables(cur, conn):
    """
    Drops each table using the queries in `drop_table_queries` list.
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates each table using the queries in `create_table_queries` list. 
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Loads the dwh configuration file and establishes connection with the redshift cluster.
    Starts by loading all of the relevant data from the desired time range into s3.
    Drops all the tables listed, creates all tables needed, and finally closes the connection. 
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
   
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    initial_copyto_s3()
    drop_tables(cur, conn)
    create_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()
