import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries
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
    for query in insert_table_queries:
        print(query)
        
        cur.execute(query)
        conn.commit()


def fill_time_table(cur, conn):
    cur.execute('SELECT ts FROM dim_time')
    t = pd.to_datetime(cur.fetchone(), unit='ms')

    cur.execute('INSERT INTO dim_time ')

    time_data = [t, t[0].hour, t[0].day, t[0].week, t[0].month, t[0].year, t[0].weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_df = pd.DataFrame.from_dict(dict(zip(column_labels, time_data)))

    print(time_data)
    print(time_df)


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    # load_staging_tables(cur, conn)
    # insert_tables(cur, conn)
    fill_time_table(cur, conn)


    conn.close()


if __name__ == "__main__":
    main()