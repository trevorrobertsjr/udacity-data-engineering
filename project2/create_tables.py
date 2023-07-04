import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries

def drop_tables(cur, conn):
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    drop_tables(cur, conn)
    create_tables(cur, conn)

# Validate I connect to database
    # try:
    #     cur.execute('SELECT datname FROM pg_catalog.pg_database;')
    #     print(cur.fetchall())
    # except psycopg2.OperationalError:
    #     pass

    conn.close()


if __name__ == "__main__":
    main()