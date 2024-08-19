import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from utilities import create_iam_role, create_redshift_cluster, delete_redshift_cluster
import pandas as pd
import boto3
import json



def drop_tables(cur, conn):
    """
    drops tables included in drop_table_queries from redshift cluster

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
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    creates tables included in create_table_queries in redshift cluster

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
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
            

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    #checking for/creating iam role
    create_iam_role(config)
    
    #checking for/creating redshift server
    create_redshift_cluster(config)

    #connecting to database
    conn = psycopg2.connect(f"host={config['CLUSTER']['Host']} dbname={config.get('CLUSTER','DWH_DB')} user={config.get('CLUSTER','DWH_DB_USER')} password={config.get('CLUSTER','DWH_DB_PASSWORD')} port={config.get('CLUSTER','DWH_PORT')}")
    cur = conn.cursor()
    
    #dropping tables
    drop_tables(cur, conn)
    #creating new ones
    create_tables(cur, conn)
    
    #close connection
    conn.close()
    
    #add in delete database thing here later 
    #delete_redshift_cluster(config)

if __name__ == "__main__":
    main()