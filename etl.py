import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
from utilities import create_iam_role, create_redshift_cluster, delete_redshift_cluster
import pandas as pd
import boto3
import json

def load_staging_tables(cur, conn):
    """
    loads data from s3 into staging tables

    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor object from psycopg2's connection.
    conn : psycopg2.connection
        Connection object of psycopg2.

    Returns
    -------
    None.

    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    inserts data from staging tables into final tables

    Parameters
    ----------
    cur : psycopg2.cursor
        Cursor object from psycopg2's connection.
    conn : psycopg2.connection
        Connection object of psycopg2.

    Returns
    -------
    None.

    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    DWH_ROLE_ARN = config['IAM_ROLE']['ARN']  
   
    
    #checking for/creating iam role
    create_iam_role(config)
    
    #checking for/creating redshift server
    create_redshift_cluster(config)

    conn = psycopg2.connect(f"host={config['CLUSTER']['Host']} dbname={config.get('CLUSTER','DWH_DB')} user={config.get('CLUSTER','DWH_DB_USER')} password={config.get('CLUSTER','DWH_DB_PASSWORD')} port={config.get('CLUSTER','DWH_PORT')}")
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()