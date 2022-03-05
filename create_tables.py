import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Description: drop all tables
    cur : cursor of database
    conn : redshift connection
    """
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Description: create all tables
    cur : cursor of database
    conn : redshift connection
    """
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    # load config from file
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    # connect to redshift database
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # reset the database, drop and create the tables
    drop_tables(cur, conn)
    create_tables(cur, conn)

    # close the database connection
    conn.close()


if __name__ == "__main__":
    main()