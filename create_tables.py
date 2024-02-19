import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops all tables defined in the drop_table_queries list.

    Parameters:
    cur (psycopg2.cursor): The cursor object for executing SQL commands.
    conn (psycopg2.connection): The connection object representing the database connection.

    Returns:
    None
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates all tables defined in the create_table_queries list.

    Parameters:
    cur (psycopg2.cursor): The cursor object for executing SQL commands.
    conn (psycopg2.connection): The connection object representing the database connection.

    Returns:
    None
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    The main function that connects to the database, drops existing tables, and creates new tables.

    Reads configuration from 'dwh.cfg' file and connects to the database using psycopg2.
    Drops existing tables using drop_tables function and creates new tables using create_tables function.
    Closes the database connection after the operations are completed.

    Returns:
    None
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()