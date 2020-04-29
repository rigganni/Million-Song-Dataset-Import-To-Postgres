import psycopg2
from sql_queries import create_table_queries, drop_table_queries
from os import environ


def create_database():
    """
    Create sparkifydb Postgres database.

    Note: host, user, password should be set up to your local environment.

    Parameters:
    None

    Returns:
    cur (psycopg2.cursor): Postgres cursor to sparkifydb
    conn (psycopg2.connection): Postgres connection to sparkifydb
    """

    # Get local development environment variables
    if environ.get("UDACITY_DATA_ENGINEER_POSTGRES_INSTANCE"):
        host = environ.get("UDACITY_DATA_ENGINEER_POSTGRES_INSTANCE")
        user = environ.get("UDACITY_DATA_ENGINEER_POSTGRES_USER")
        password = environ.get("UDACITY_DATA_ENGINEER_POSTGRES_PASSWORD")
    dbname = "studentdb"
    # connect to default database
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(host, dbname, user, password))
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    # create sparkify database with UTF8 encoding
    cur.execute("DROP DATABASE IF EXISTS sparkifydb")
    cur.execute("CREATE DATABASE sparkifydb WITH ENCODING 'utf8' TEMPLATE template0")

    # close connection to default database
    conn.close()

    # connect to sparkify database
    dbname = "sparkifydb"
    conn = psycopg2.connect("host={} dbname={} user={} password={}".format(host, dbname, user, password))
    cur = conn.cursor()

    return cur, conn


def drop_tables(cur, conn):
    """
    Drop all existing tables in sparkifydb.

    Parameters:
    cur (psycopg2.cursor): Postgres cursor to sparkifydb
    conn (psycopg2.connection): Postgres connection to sparkifydb

    Returns:
    None
    """

    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Create tables in sparkifydb.

    Parameters:
    cur (psycopg2.cursor): Postgres cursor to sparkifydb
    conn (psycopg2.connection): Postgres connection to sparkifydb

    Returns:
    None
    """

    for query in create_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Create or recreate sparkify db with all necessary objects.

    Parameters:
    None

    Returns:
    None
    """

    cur, conn = create_database()

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()
