import configparser
import psycopg2
from sql_queries import table_size_queries, duplicate_queries, tables, keys
from utilities import delete_redshift_cluster
import pandas as pd
import boto3
import json

def test_table_size(cur, tables):
    """
    tests and prints table size 

    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor object from psycopg2's connection.
    tables : list
        list of tables

    Returns
    -------
    None.

    """
    for i, query in enumerate(table_size_queries):
        cur.execute(query)
        result = cur.fetchone()
        print(f"Table {tables[i]} has {result[0]} rows.")
        
def test_duplicates(cur, tables, keys): 
    """
    tests and prints duplicates 

    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor object from psycopg2's connection.
    tables : list
        list of tables
    keys: list
        list of keys to check for duplicates

    Returns
    -------
    None.

    """
    duplicates = 0
    for i, query in enumerate(duplicate_queries):
        print(f"Checking for duplicates in table: {tables[i]} using key: {keys[i]}")
        cur.execute(query)
        results = cur.fetchall()
        if results:
            duplicates += 1
            for row in results:
                print(f"{keys[i]}: {row[0]}, Count: {row[1]}")
        else:
            print(f"No duplicates found in table {tables[i]} using key {keys[i]}.")
        print()
    return duplicates

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_ROLE_ARN = config['IAM_ROLE']['ARN']  

    conn = psycopg2.connect(f"host={config['CLUSTER']['Host']} dbname={config.get('CLUSTER','DWH_DB')} user={config.get('CLUSTER','DWH_DB_USER')} password={config.get('CLUSTER','DWH_DB_PASSWORD')} port={config.get('CLUSTER','DWH_PORT')}")
    cur = conn.cursor()
                            
    test_table_size(cur, tables)
    #duplicates = test_duplicates(cur, tables, keys)
                            
    conn.close()
    
    #print(f"{duplicates} tables have duplicates in them")
    print("ETL process completed successfully, removing cluster") 
    delete_redshift_cluster(config)
                            
if __name__ == "__main__":
    main()
                        