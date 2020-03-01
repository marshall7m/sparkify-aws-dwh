import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
import pandas as pd
from time import time

def load_staging_tables(cur, conn):
    print('Loading Staging Tables')
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    # init dict to record query runtime
    insert_query_time_dict = {table:None for table, query in insert_table_queries.items()}
    print('Inserting into Tables:')
    # for every insert query, execute query and record runtime
    for table,query in insert_table_queries.items():
        print('\t' + table)
        t0 = time()
        cur.execute(query)
        conn.commit()
        queryTime = time()-t0
        insert_query_time_dict[table] = queryTime
    
    print(insert_query_time_dict)

def main():
    config = configparser.ConfigParser()
    # Get config values
    config.read('dwh.cfg')

    # Initiate postgres connection
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    # load staging tables from s3
    load_staging_tables(cur, conn)
    # insert staging data into fact and dimension tables
    insert_tables(cur, conn)
    
    conn.close()

if __name__ == "__main__":
    main()