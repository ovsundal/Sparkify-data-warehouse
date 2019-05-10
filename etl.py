import configparser
import psycopg2
import pandas as pd

from setup_redshift_cluster import create_clients
from sql_queries import copy_table_queries, insert_table_queries

def retrieve_data_from_bucket():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION = config.get('CLUSTER', 'REGION')
    BUCKET_NAME = config.get('S3', 'BUCKET_NAME')

    iam, ec2, s3 = create_clients(KEY, SECRET, REGION)
    song_files = open("song_data.txt", "ab")
    log_files = open("log_data.txt", "ab")
    data_bucket = s3.Bucket('udacity-dend')

    # download songs
    for sample in data_bucket.objects.filter(Prefix="song"):
        entry = sample.get()['Body'].read()
        print(entry)
        song_files.write(entry)

    # download logs
    for sample in data_bucket.objects.filter(Prefix="log"):
        entry = sample.get()['Body'].read()
        print(entry)
        log_files.write(entry)

    s3.upload_file(song_files, BUCKET_NAME, 'song_data')
    s3.upload_file(log_files, BUCKET_NAME, 'log_data')


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print('in load')
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()

    retrieve_data_from_bucket()
    # load_staging_tables(cur, conn)
    # insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()