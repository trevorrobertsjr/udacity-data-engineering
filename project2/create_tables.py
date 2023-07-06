import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    """
    This function drops any existing staging or fact/dimension tables
    to prepare the environment for a fresh deployment

    Parameters:
    argument1 (psycopg2 cursor): The psycopg2 object created by the connect command that allows us to execute queries
    argument2 (psycopg2 connection): The psycopg2 object that establishes the database connection.

    Returns:
    None

   """
    # Iterate through and execute each query in the drop_table_queries list from sql_queries.py
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    """
    This function create the staging, fact, and dimension tables 
    to prepare the environment for data load and transformations.

    Parameters:
    argument1 (psycopg2 cursor): The psycopg2 object created by the connect command that allows us to execute queries
    argument2 (psycopg2 connection): The psycopg2 object that establishes the database connection.

    Returns:
    None

   """
    # Iterate through and execute each query in the create_table_queries list from sql_queries.py
    for query in create_table_queries:
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()