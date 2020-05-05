# -*- coding: utf-8 -*-

from dotenv import load_dotenv
load_dotenv('.env')

import logging
logging.basicConfig(filename='slave.process.access.log', level=logging.INFO)

import os
import subprocess

from dbio import *

image_path = os.getenv('IMAGES_PATH')
slave_path = os.getenv('SLAVES_PATH') #'/mnt/sentinel1/slaves/'
graph_path = os.getenv('PROCESSING_XML')
gpt_path = os.getenv('GPT_PATH')

exec_cmd = '%s %s -t %s{}_Orb_Cal_ML_TF.dim {}{}.zip' % (gpt_path, graph_path, slave_path)


def save_information(title):
    query = "UPDATE sentinel1 SET processed=TRUE WHERE slave=TRUE and title='{}'".format(title)
    update_query(query)


def set_path():
    os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(os.getcwd())
    os.chdir(path)
    return path


def get_slaves():
    query = "SELECT title, footprint FROM sentinel1 WHERE processed=FALSE and slave=TRUE;"
    data = get_query(query)
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
