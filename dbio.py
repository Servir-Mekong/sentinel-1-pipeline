# -*- coding: utf-8 -*-

from dotenv import load_dotenv
load_dotenv('.env')

import logging
logging.basicConfig(filename='logs/db.connection.log', level=logging.WARNING)

import os
import psycopg2


db = {
    'dbname': os.getenv('DB_NAME'),
    'user': os.getenv('DB_USER'),
    'host': os.getenv('DB_HOST'),
    'password': os.getenv('DB_PASSWORD'),
    'port': os.getenv('DB_PORT'),
}


def connect_to_db():
    try:
        connection_parameters = 'dbname={} user={} host={} password={} port={}'.format(
            db['dbname'], db['user'], db['host'], db['password'], db['port']
        )
        conn = psycopg2.connect(connection_parameters)
    except Exception as e:
        print('problem connecting to the database!')
        logging.error(e)
    else:
        return conn, conn.cursor()


def close_connection(cur, conn):
    cur.close()
    conn.close()


def get_query(query):
    conn, cur = connect_to_db()
    cur.execute(query)
    data = cur.fetchall()
    close_connection(conn, cur)
    return data


def update_query(query):
    conn, cur = connect_to_db()
    cur.execute(query)
    conn.commit()
    close_connection(conn, cur)
