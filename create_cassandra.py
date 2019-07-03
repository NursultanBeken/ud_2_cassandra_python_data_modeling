# Import Python packages 
import pandas as pd
import cassandra
import re
import os
import glob
import numpy as np
import json
import csv
from cassandra.cluster import Cluster

"""
    This procedure connects to a locally installed Apache Cassandra instance.
    It creates a Keyspace and sets keyspace for our session.
"""
def create_and_set_keyspace():
    try:
        cluster = Cluster(['127.0.0.1']) #If you have a locally installed Apache Cassandra instance
        session = cluster.connect()
    except Exception as e:
        print(e)  
    try:
        session.execute("""
            CREATE KEYSPACE IF NOT EXISTS udacity 
            WITH REPLICATION = 
            { 'class' : 'SimpleStrategy', 'replication_factor' : 1 }"""
        )
    except Exception as e:
        print(e)
    try:
        session.set_keyspace('udacity')
    except Exception as e:
        print(e)

def create_tables():
    create_query_1 = "CREATE TABLE IF NOT EXISTS music_library "
    create_query_1 = create_query_1 + "(sessionId int, itemInSession int, artist text, song text, length double ,PRIMARY KEY (sessionId, itemInSession ))"
    
    create_query_2 = "CREATE TABLE IF NOT EXISTS user_library"
    create_query_2 = create_query_2 + "(userid int, sessionid int, itemInSession int, firstname text, lastname text, artist text, song text, PRIMARY KEY (userid, sessionid, itemInSession ))"
    
    create_query_3 = "CREATE TABLE IF NOT EXISTS music_library"
    create_query_3 = create_query_3 + "(song text, userid int, firstname text, lastname text, PRIMARY KEY (song, userid ))"

    try:
        session.execute(create_query_1)
        session.execute(create_query_2)
        session.execute(create_query_3)
    except Exception as e:
        print(e) 
      
def insert_table(file_name):

    insert_query_1 = "INSERT INTO song_library (sessionId, itemInSession, artist, song,length )"
    insert_query_1 = insert_query_1 + "VALUES (%s, %s, %s, %s , %s)"

    insert_query_2 = "INSERT INTO user_library (userid,sessionid,itemInSession,firstname,lastname,artist,song)"
    insert_query_2 = insert_query_2 + "VALUES (%s, %s, %s, %s , %s, %s, %s)"
    
    insert_query_3 = "INSERT INTO music_library (song, userid, firstname,lastname  )"
    insert_query_3 = insert_query_3 + "VALUES (%s, %s, %s, %s )"

    with open(file, encoding = 'utf8') as f:
        csvreader = csv.reader(f)
        next(csvreader) # skip header
        for line in csvreader:
            session.execute(insert_query_1, (int(line[8]), int(line[3]), line[0], line[9], float(line[5]) ))
            session.execute(insert_query_2, (int(line[10]),int(line[8]), int(line[3]), line[1], line[4], line[0], line[9]  ))  
            session.execute(insert_query_3, (line[9] ,int(line[10]), line[1], line[4],))  

def select():

    query_1 = "select artist, song, length from song_library WHERE sessionid = 338 and iteminsession=4"
    try:
        rows = session.execute(query_1)
    except Exception as e:
        print(e)
    for row in rows:
        print (row.artist, row.song, row.length)  

    query_2 = "select artist, song, firstname, lastname from user_library WHERE userid = 10 and sessionid = 182"
    try:
        rows = session.execute(query_2)
    except Exception as e:
        print(e)
    for row in rows:
        print (row.artist, row.song, row.firstname, row.lastname)  

    query_3 = "select firstname, lastname from music_library WHERE song ='All Hands Against His Own' "
    try:
        rows = session.execute(query_3)
    except Exception as e:
        print(e)
    for row in rows:
        print (row.firstname, row.lastname, row.song)   

def main():
    create_and_set_keyspace() 
    create_tables()
    insert_table()
if __name__ == "__main__":
    main()