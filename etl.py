import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries, select_time_table, truncate_time_table, \
    time_table_full_insert
import pandas as pd


def load_staging_tables(cur, conn):
    try:
        for query in copy_table_queries:
            print(query)

            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print(e)
        print('Error: could not load staging tables')


def insert_tables(cur, conn):
    try:
        for query in insert_table_queries:
            print(query)

            cur.execute(query)
            conn.commit()
    except psycopg2.Error as e:
        print('Error inserting into table')
        print(e)


def fill_time_table(cur, conn):
    """
    Extracts timestamp value from table, derives time values, truncates the old table and inserts a new row with all time data
    :param cur:
    :param conn:
    """
    cur.execute(select_time_table)
    raw_data = list(cur.fetchall())
    print('Retrieving time data...')
    time_data = []

    for row in raw_data:
        t = pd.to_datetime(row[0], unit='ms')
        time_data.append([row[0], t.hour, t.day, t.week, t.month, t.year, t.weekday()])

    cur.execute(truncate_time_table)
    print('Truncating table and inserting new rows with time required units...')

    for entry in time_data:
        try:
            cur.execute(time_table_full_insert, entry)
            conn.commit()
        except psycopg2.Error as e:
            print('Error inserting row')
            print(e)



def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    fill_time_table(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()