import configparser

# CONFIGURATION

config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

players_table_drop = "DROP TABLE IF EXISTS players"
teams_table_drop = "DROP TABLE IF EXISTS teams"
games_table_drop = "DROP TABLE IF EXISTS games"
playerstats_table_drop = "DROP TABLE IF EXISTS playerstats"
teamstats_table_drop = "DROP TABLE IF EXISTS teamstats"
contracts_table_drop = "DROP TABLE IF EXISTS contracts"

staging_playerstats_drop = "DROP TABLE IF EXISTS staging_playerstats"
staging_teamstats_drop = "DROP TABLE IF EXISTS staging_teamstats"
staging_players_drop = "DROP TABLE IF EXISTS staging_players"
staging_teams_drop = "DROP TABLE IF EXISTS staging_teams"
staging_team_roster_table_drop = "DROP TABLE IF EXISTS staging_team_roster"
staging_games_drop = "DROP TABLE IF EXISTS staging_games"
staging_contracts_drop = "DROP TABLE IF EXISTS staging_contracts"

# RECORD COUNT

player_count = "SELECT COUNT(*) as record_cnt FROM players"
team_count = "SELECT COUNT(*) as record_cnt FROM teams"
game_count = "SELECT COUNT(*) as record_cnt FROM games"
playerstats_count = "SELECT COUNT(*) as record_cnt FROM player_stats"
teamstats_count = "SELECT COUNT(*) as record_cnt FROM team_stats"
contracts_count = "SELECT COUNT(*) as record_cnt FROM contracts"

# RECORD PREVIEW

player_preview = "SELECT * FROM players LIMIT 10"
team_preview = "SELECT * FROM teams LIMIT 10"
game_preview = "SELECT * FROM games LIMIT 10"
playerstats_preview = "SELECT * FROM player_stats LIMIT 10"
teamstats_preview = "SELECT * FROM team_stats LIMIT 10"
contracts_preview = "SELECT * FROM contracts LIMIT 10"

# CREATE TABLES

staging_playerstats_create= ("""
CREATE TABLE IF NOT EXISTS staging_playerstats(
game_id int,
team_id int,
team_abbreviation varchar(10),
team_city varchar(50),
player_id int,
player_name varchar(300),
nickname varchar(300),
start_position varchar(5),
comment varchar(100),
min varchar(10),
fg numeric,
fga numeric,
fg_pct numeric,
fg3m numeric,
fg3a numeric,
fg3_pct numeric,
ftm numeric,
fta numeric,
ft_pct numeric,
oreb numeric,
dreb numeric,
reb numeric,
ast numeric,
stl numeric,
blk numeric,
tov numeric,
pf numeric,
pts numeric,
plus_minus numeric
)
""")



staging_teamstats_create = ("""
CREATE TABLE IF NOT EXISTS staging_teamstats(
season_id int,
team_id int,
team_abbreviation varchar(10),
team_name varchar(300),
game_id int,
game_date varchar(250),
matchup varchar(300),
wl varchar(5),
min varchar(10),
pts numeric,
fgm numeric,
fga numeric,
fg_pct numeric,
fg3m numeric,
fg3a numeric,
fg3_pct numeric,
ftm numeric,
fta numeric,
ft_pct numeric,
oreb numeric,
dreb numeric,
reb numeric,
ast numeric,
stl numeric,
blk numeric,
tov numeric,
pf numeric,
plus_minus numeric
)
""")

staging_games_create= ("""
CREATE TABLE IF NOT EXISTS staging_games(
season_id int,
game_id int,
game_date varchar(250),
matchup varchar(100),
home_team varchar(50),
away_team varchar(50)
)
""")

staging_players_create= ("""
CREATE TABLE IF NOT EXISTS staging_players(
player_id int,
full_name varchar(300),
first_name varchar(300),
last_name varchar(300),
is_active varchar(250)
)
""")

staging_teams_create= ("""
CREATE TABLE IF NOT EXISTS staging_teams(
team_id int,
full_name varchar(300),
abbreviation varchar(25),
nickname varchar(300),
city varchar(50),
state varchar(50),
year_founded int
)
""")

staging_team_roster_create = ("""
CREATE TABLE IF NOT EXISTS staging_team_roster(
team_id int,
season int,
league_id int,
player varchar(250),
nickname varchar(250),
player_slug varchar(250),
num int,
position varchar(10),
height varchar(10),
weight int,
birth_date varchar(250),
age numeric,
exp varchar(5),
school varchar(250),
player_id int
)
""")

staging_contracts_create= ("""
CREATE TABLE IF NOT EXISTS staging_contracts(
player varchar(300),
tm varchar(25),
_2021_22 varchar(300),
_2022_23 varchar(300),
_2023_24 varchar(300),
_2024_25 varchar(300),
_2025_26 varchar(300),
_2026_27 varchar(300),
signed_using varchar(300),
guaranteed varchar(300)
)
""")


games_table_create = ("""
CREATE TABLE IF NOT EXISTS games(
game_id int PRIMARY KEY,
season_id int,
game_date date,
matchup varchar(50),
home_team_id int,
away_team_id int)
DISTSTYLE AUTO;
""")

players_table_create = ("""
CREATE TABLE IF NOT EXISTS players(
player_id int PRIMARY KEY,
last_name varchar(100),
first_name varchar(100),
is_active boolean,
current_team_id varchar(100),
num int,
position varchar(10),
height varchar(10),
weight varchar(10),
age numeric,
exp int,
school varchar(250))
DISTSTYLE ALL;
""")

teams_table_create = ("""
CREATE TABLE IF NOT EXISTS teams(
team_id int PRIMARY KEY,
team_city varchar(200),
team_state varchar(200),
team_nickname varchar(200),
team_ab varchar(50))
DISTSTYLE ALL;
""")


contracts_table_create = ("""
CREATE TABLE IF NOT EXISTS contracts(
player_id int PRIMARY KEY,
team_id int,
season_id int,
salary int)
DISTSTYLE ALL;
""")

player_stats_table_create = ("""
CREATE TABLE IF NOT EXISTS player_stats(
playerstats_id int IDENTITY(1,1),
game_id int SORTKEY,
player_id int DISTKEY,
team_id int,
min numeric,
fga int,
fg int,
fg3a int,
fg3m int,
fta int,
ftm int,
ast int,
tov int,
oreb int,
dreb int,
stl int,
blk int,
pf int)
DISTSTYLE KEY;
""")

team_stats_table_create = ("""
CREATE TABLE IF NOT EXISTS team_stats(
teamstats_id int IDENTITY(1,1),
game_id int SORTKEY,
team_id int DISTKEY,
min numeric,
fga int,
fg int,
fg3a int,
fg3m int,
fta int,
ftm int,
ast int,
tov int,
oreb int,
dreb int,
stl int,
blk int,
pf int)
DISTSTYLE KEY;
""")

######## COPY DATA TO STAGING TABLES ###########

staging_contracts_copy = ("""
COPY staging_contracts
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
REGION 'us-west-2';
""").format(config['S3']['CONTRACTS_DATA'], config['IAM_ROLE']['ARN'])

staging_games_copy = ("""
COPY staging_games
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
GZIP 
IGNOREHEADER 1
REGION 'us-west-2';
""").format(config['S3']['GAME_DATA'], config['IAM_ROLE']['ARN'])

staging_team_roster_copy = ("""
COPY staging_team_roster
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
GZIP 
IGNOREHEADER 1
REGION 'us-west-2';
""").format(config['S3']['ROSTER_DATA'], config['IAM_ROLE']['ARN'])

staging_teams_copy = ("""
COPY staging_teams
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
GZIP 
IGNOREHEADER 1
REGION 'us-west-2';
""").format(config['S3']['TEAM_DATA'], config['IAM_ROLE']['ARN'])

staging_players_copy = ("""
COPY staging_players
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
GZIP
IGNOREHEADER 1
REGION 'us-west-2';
""").format(config['S3']['PLAYER_DATA'], config['IAM_ROLE']['ARN'])

staging_teamstats_copy = ("""
COPY staging_teamstats
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
GZIP
IGNOREHEADER 1
REGION 'us-west-2';
""").format(config['S3']['TEAMSTATS_DATA'], config['IAM_ROLE']['ARN'])

staging_playerstats_copy = ("""
COPY staging_playerstats
FROM {}
IAM_ROLE {} 
FORMAT AS CSV
GZIP 
IGNOREHEADER 1
REGION 'us-west-2';
""").format(config['S3']['PLAYERSTATS_DATA'], config['IAM_ROLE']['ARN'])

# INSERT RECORDS

games_table_insert = ("""
INSERT INTO games(
game_id,
season_id,
game_date,
matchup,
home_team_id,
away_team_id
)
SELECT DISTINCT
s1.game_id,
s1.season_id,
CAST(s1.game_date AS DATE) AS game_date,
s1.matchup,
s2.team_id AS home_team_id,
s3.team_id AS away_team_id
FROM staging_games AS s1
LEFT JOIN staging_teams AS s2
ON s1.home_team = s2.abbreviation
LEFT JOIN staging_teams AS s3
ON s1.away_team = s3.abbreviation;
""")

contracts_table_insert = ("""
INSERT INTO contracts(
player_id,
team_id,
season_id,
salary
)
SELECT DISTINCT
s1.player_id,
s4.team_id as team_id,
22021 as season_id,
CASE s3.salary
    WHEN ' '
        THEN 0::int
    ELSE CAST(REPLACE(LTRIM(s3.salary,'$'),',','') as int)
END as salary
FROM (SELECT s2.player as player_name, _2021_22 as salary, tm as team_ab
FROM staging_contracts as s2) as s3
LEFT JOIN staging_players as s1
ON lower(s3.player_name) = lower(s1.full_name)
LEFT JOIN staging_teams as s4
ON s3.team_ab=s4.abbreviation
WHERE s1.is_active = 'True'
AND s1.player_id IS NOT NULL;
""")

team_table_insert = ("""
INSERT INTO teams(
team_id,
team_city,
team_state,
team_nickname,
team_ab
)
SELECT DISTINCT
s1.team_id,
s1.city as team_city,
s1.state as team_state,
s1.nickname as team_nickname,
s1.abbreviation as team_ab
FROM staging_teams as s1;
""")

player_table_insert = ("""
INSERT INTO players(
player_id,
last_name,
first_name,
is_active,
current_team_id,
num,
position,
height,
weight,
age,
exp,
school
)
SELECT DISTINCT
s1.player_id,
s1.last_name,
s1.first_name,
CASE LEFT(LOWER(s1.is_active),5)
    WHEN 'false'
        THEN 0::boolean
    ELSE 1::boolean
END as is_active,
s2.team_id as current_team_id,
s2.num,
s2.position,
s2.height,
s2.weight,
s2.age,
CASE LEFT(LOWER(s2.exp),1)
    WHEN 'r'
        THEN 0::int
    ELSE s2.exp::int
END AS exp,
s2.school
FROM staging_players as s1
LEFT JOIN staging_team_roster as s2 on s1.player_id = s2.player_id;
""")

player_stats_table_insert = ("""
INSERT INTO player_stats(
game_id,
player_id,
team_id,
min,
fga,
fg,
fg3a,
fg3m,
fta,
ftm,
ast,
tov,
oreb,
dreb,
stl,
blk,
pf
)
SELECT DISTINCT
s1.game_id,
s1.player_id,
s1.team_id,
CASE s1.min
    WHEN ''
        THEN NULL::numeric
    ELSE CAST(SPLIT_PART(s1.min,':',1) as numeric)+CAST(SPLIT_PART(s1.min,':',2) AS numeric)/60
END as min,
CAST(s1.fga AS INT),
CAST(s1.fg AS INT),
CAST(s1.fg3a AS INT),
CAST(s1.fg3m AS INT),
CAST(s1.fta AS INT),
CAST(s1.ftm AS INT),
CAST(s1.ast AS INT),
CAST(s1.tov AS INT),
CAST(s1.oreb AS INT),
CAST(s1.dreb AS INT),
CAST(s1.stl AS INT),
CAST(s1.blk AS INT),
CAST(s1.pf AS INT)
FROM staging_playerstats as s1
LEFT JOIN staging_games as s2 ON s1.game_id = s2.game_id;
""")

team_stats_table_insert = ("""
INSERT INTO team_stats(
game_id,
team_id,
min,
fga,
fg,
fg3a,
fg3m,
fta,
ftm,
ast,
tov,
oreb,
dreb,
stl,
blk,
pf
)
SELECT DISTINCT
s1.game_id,
s1.team_id,
s1.min as numeric,
CAST(s1.fga AS INT),
CAST(s1.fgm AS INT),
CAST(s1.fg3a AS INT),
CAST(s1.fg3m AS INT),
CAST(s1.fta AS INT),
CAST(s1.ftm AS INT),
CAST(s1.ast AS INT),
CAST(s1.tov AS INT),
CAST(s1.oreb AS INT),
CAST(s1.dreb AS INT),
CAST(s1.stl AS INT),
CAST(s1.blk AS INT),
CAST(s1.pf AS INT)
FROM staging_teamstats as s1;
""")


# QUERY LISTS

create_table_queries = [staging_playerstats_create, staging_teamstats_create, staging_games_create, staging_players_create, staging_teams_create,
                        staging_team_roster_create, staging_contracts_create, games_table_create, players_table_create, teams_table_create,
                     contracts_table_create, player_stats_table_create, team_stats_table_create]
drop_table_queries = [players_table_drop,teams_table_drop,games_table_drop,playerstats_table_drop,teamstats_table_drop,
                      contracts_table_drop,staging_playerstats_drop,staging_teamstats_drop,staging_players_drop,staging_teams_drop,
                      staging_team_roster_table_drop,staging_games_drop,staging_contracts_drop]
insert_table_queries = [contracts_table_insert, games_table_insert,team_table_insert,player_table_insert,player_stats_table_insert, team_stats_table_insert]
staging_copy_queries = [staging_contracts_copy, staging_games_copy, staging_team_roster_copy, staging_teams_copy, staging_players_copy, staging_teamstats_copy, staging_playerstats_copy]
data_check_1 = [player_count,team_count,game_count,playerstats_count,teamstats_count,contracts_count]
data_check_2 = [player_preview, team_preview, game_preview, playerstats_preview,teamstats_preview, contracts_preview]
