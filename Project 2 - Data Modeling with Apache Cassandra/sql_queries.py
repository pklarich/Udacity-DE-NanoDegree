# CREATE TABLES

session_create = ("""
CREATE TABLE IF NOT EXISTS
session_library(
artist text, 
itemInSession int,
length decimal,
sessionId int,
song text,
PRIMARY KEY (sessionId, itemInSession)
)
""")

user_create = ("""
CREATE TABLE IF NOT EXISTS 
user_library(
artist text, 
song text, 
firstName text, 
itemInSession int, 
lastName text, 
sessionId int, 
userId int, 
PRIMARY KEY ((userId,sessionId), itemInSession)
)
""")

song_create = ("""
CREATE TABLE IF NOT EXISTS 
song text,
sessionId int,
itemInSession int,
song_library(
firstName text,
lastName text,
PRIMARY KEY (song,sessionId,itemInSession)
)
""")

# DROP TABLES

session_drop = ("""DROP TABLE IF EXISTS session_library""")

user_drop = ("""DROP TABLE IF EXISTS user_library""")

song_drop = ("""DROP TABLE IF EXISTS song_library""")


# INSERT RECORDS

session_insert =("""
INSERT INTO 
session_library(
sessionId, 
itemInSession, 
artist, 
length, 
song
) 
VALUES(%s,%s,%s,%s,%s)
""")
user_insert =   ("""
INSERT INTO user_library(
userId
sessionId, 
itemInSession, 
artist, 
song, 
firstName, 
lastName, 
) 
VALUES(%s,%s,%s,%s,%s,%s,%s)
""")

song_insert =   ("""
INSERT INTO song_library(
song, 
sessionId, 
itemInSession
firstName, 
lastName, 
) 
VALUES(%s,%s,%s,%s,%s)
""")

# TEST QUERIES

session_query = (""" 
SELECT artist, song, length 
FROM session_library 
WHERE sessionId = 338 AND itemInSession = 4
""")

user_query = ("""
SELECT artist, song, firstName, lastName 
FROM user_library 
WHERE userId = 10 AND sessionId = 182
""")

song_query = ("""
SELECT firstName, lastName 
FROM song_library 
WHERE song = 'All Hands Against His Own'
""")

query1 = ("""
Give me the artist, song title and song's length in the music app history
that was heard during sessionId = 338, and itemInSession = 4
""")

query2 = ("""
Give me only the following: name of artist, song (sorted by itemInSession)
and user (first and last name) for userid = 10, sessionid = 182
""")

query3 = ("""
Give me every user name (first and last) in my music app history who listened
to the song 'All Hands Against His Own'
""")

drop_table_queries = {'session_drop': session_drop, 'user_drop': user_drop, 'song_drop': song_drop}
create_table_queries = {'session_create': session_create, 'user_create':user_create, 'song_create':song_create}
insert_queries = {'session_insert':session_insert,'user_insert': user_insert,'song_insert': song_insert}
check_queries = {query1: session_query, query2: user_query, query3: song_query}