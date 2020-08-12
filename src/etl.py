import pandas as pd
import numpy as np
import configparser
import psycopg2
from psycopg2 import sql
from sql_queries import copy_table_queries, insert_table_queries
from time import time

def load_staging_tables(cur, conn):
    """Copy the song and event JSON data from S3 into staging tables in Redshift"""
    for query in copy_table_queries:
        print(query)
        t0 = time()
        cur.execute(query)
        conn.commit()
        loadTime = time()-t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))

def insert_tables(cur, conn):
    """Load data from the staging tables into the final analytics tables in Redshift"""
    for query in insert_table_queries:
        print(query)
        t0 = time()
        cur.execute(query)
        conn.commit()
        loadTime = time() - t0
        print("=== DONE IN: {0:.2f} sec\n".format(loadTime))
        
def quality_check_data(cur, conn, tablename, idcol):
    """
    Performs the following tasks:
    1. Print the total number of rows in the table
    2. Remove any duplicate rows from the table using SQL logic to manually find and delete duplicate ids
        - print how many rows were removed
    Input params:
        cur / conn: the psycopg2 connection and cursor objects
        table: the tablename (string)
        idcol: the primary key column of the table (string)
    """   
    from sql_queries import remove_duplicate_rows
    table_rows = cur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(tablename)))
    table_rows = cur.fetchone()
    print("There are {} rows in the {} table".format(table_rows[0], tablename))
    # Execute remove duplicate rows query
    cur.execute(sql.SQL(remove_duplicate_rows).format(table=sql.Identifier(tablename), pkey=sql.Identifier(idcol)))
    table_rows2 = cur.execute(sql.SQL("SELECT COUNT(*) FROM {};").format(sql.Identifier(tablename)))
    table_rows2 = cur.fetchone()
    print("There are {} rows in the {} table after removing duplicates".format(table_rows2[0], tablename))

def main():
    """
    1. Connect and create cursor to the redshift cluster (based on config file parameters)
    2. Copy the song and event JSON data from S3 into staging tables in Redshift
    3. Load data from the staging tables into the final analytics tables in Redshift
    4. Remove any duplicate rows from the analytics tables
    5. Check that the unique user_ids match between the users and songplays tables
    6. Check that the unique start_times match between the time and songplays tables
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load staging tables
    #load_staging_tables(cur, conn)
    
    # load analytics tables
    insert_tables(cur, conn)

    # Check for and remove duplicate rows
    quality_check_data(cur, conn, tablename='songplays', idcol='songplay_id')
    quality_check_data(cur, conn, tablename='songs', idcol='song_id')
    quality_check_data(cur, conn, tablename='artists', idcol='artist_id')
    quality_check_data(cur, conn, tablename='time', idcol='start_time')
    quality_check_data(cur, conn, tablename='users', idcol='user_id')
    
    # Check that the unique user_ids in the user and songplay table match
    cur.execute("SELECT DISTINCT(user_id) FROM songplays;")
    uid_songplays = [r[0] for r in cur.fetchall()]
    print("{} unique user_ids in songplays table".format(len(uid_songplays)))
    cur.execute("SELECT DISTINCT(user_id) FROM users;")
    uid_users = [r[0] for r in cur.fetchall()]
    print("{} unique user_ids in users table".format(len(uid_users)))
    
    # Return the user_ids in users table with no "NextSong" clicks
    diff = np.setdiff1d(uid_users, uid_songplays)
    print("The following users have no 'NextSong' clicks: user ids {}".format(str(diff)))
    
    # Check that the unique times in the time and songplays table match
    cur.execute("SELECT DISTINCT(start_time) FROM songplays;")
    ts_songplays = [r[0] for r in cur.fetchall()]
    print("{} unique times in songplays table".format(len(ts_songplays)))
    cur.execute("SELECT DISTINCT(start_time) FROM time;")
    ts_time = [r[0] for r in cur.fetchall()]
    print("{} unique times in time table".format(len(ts_time)))
    
    conn.close()

if __name__ == "__main__":
    main()
    