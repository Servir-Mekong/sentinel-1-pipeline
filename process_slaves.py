import logging
logging.basicConfig(filename='slave.process.access.log', level=logging.INFO)

import os
import psycopg2
import subprocess

image_path = '/mnt/sentinel1/images/'
slave_path = '/mnt/sentinel1/slaves/'
graph_path = '/mnt/sentinel1/pipeline.xml'
gpt_path = '/usr/local/snap/bin/gpt'

exec_cmd = '%s %s -t %s{}_Orb_Cal_ML_TF.dim {}{}.zip' % (gpt_path, graph_path, slave_path)

# use port if hosted in some other port than 5432
db = {
    'dbname'  : 'db-name',
    'user'    : 'user-name',
    'host'    : 'localhost',
    'password': 'db-password'
}

def connect_to_db(db):
    try:
        connection_parameters = 'dbname=%s user=%s host=%s password=%s' % (db['dbname'], db['user'], db['host'], db['password'])
        conn = psycopg2.connect(connection_parameters)
    except Exception as e:
        print('problem connecting to the database!')
        logging.error(e)
    else:
        return conn, conn.cursor()

def close_connection(cur, conn):
    cur.close()
    conn.close()

def save_information(title):
    conn, cur = connect_to_db(db)
    cur.execute("UPDATE sentinel1 SET processed=TRUE WHERE slave=TRUE and title='{}'".format(title))
    conn.commit()
    close_connection(conn, cur)

def set_path():
    os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(os.getcwd())
    os.chdir(path)
    return path

def get_slaves():
    conn, cur = connect_to_db(db)
    cur.execute("SELECT title, footprint FROM sentinel1 WHERE processed=FALSE and slave=TRUE;")
    data = cur.fetchall()
    close_connection(conn, cur)
    output = []
    for _data in data:
        output.append(_data[0])
    return output

def main():
    set_path()
    slaves = get_slaves()
    for slave in slaves:
        try:
            print('*************************************************')
            print('started file: {}'.format(slave))
            slave_processing = exec_cmd.format(slave, image_path, slave)
            result = subprocess.check_output(slave_processing, shell=True)
            print(result)
            save_information(slave)
            print('end file: {}'.format(slave))
            print('*************************************************')
        except Exception as e:
            logging.error('problem processing file: {} because {}'.format(slave, e))
            print('problem processing file: {} because {}'.format(slave, e))
            continue

if __name__ == '__main__':
    main()
