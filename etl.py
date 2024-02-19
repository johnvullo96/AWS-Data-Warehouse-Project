import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads data from S3 into staging tables.

    Parameters:
    cur (psycopg2.cursor): The cursor object for executing SQL commands.
    conn (psycopg2.connection): The connection object representing the database connection.

    Returns:
    None
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    Inserts data from staging tables into analytics tables.

    Parameters:
    cur (psycopg2.cursor): The cursor object for executing SQL commands.
    conn (psycopg2.connection): The connection object representing the database connection.

    Returns:
    None
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main function that connects to the database, loads data into staging tables, and inserts into analytics tables.

    Reads configuration from 'dwh.cfg' file and connects to the database using psycopg2.
    Loads data into staging tables using load_staging_tables function and inserts into analytics tables using insert_tables function.
    Closes the database connection after the operations are completed.

    Returns:
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()