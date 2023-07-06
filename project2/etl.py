import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads the source data in to the staging tables.

    Parameters:
    argument1 (psycopg2 cursor): The psycopg2 object created by the connect command that allows us to execute queries
    argument2 (psycopg2 connection): The psycopg2 object that establishes the database connection.

    Returns:
    None

   """
    # Iterate through and execute each query in the copy_table_queries list from sql_queries.py to load the raw data into the staging tables
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()

def insert_tables(cur, conn):
    """
    This function loads the staging data to the fact and dimension tables.

    Parameters:
    argument1 (psycopg2 cursor): The psycopg2 object created by the connect command that allows us to execute queries
    argument2 (psycopg2 connection): The psycopg2 object that establishes the database connection.

    Returns:
    None

   """
    # Iterate through and execute each query in the insert_table_queries list from sql_queries.py to load the staging data into the fact and dimension tables
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    """
    main() loads the database connection information from dwh.cfg, establishes
    the database connection and cursor, and runs the drop_tables and create_tables functions.

    Parameters:
    None

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