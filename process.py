# -*- coding: utf-8 -*-

from dotenv import load_dotenv
load_dotenv('.env')

import logging
logging.basicConfig(filename='logs/process.log', level=logging.INFO)

import os
import sys
sys.path.append(os.getenv('SNAPPY_PATH'))
from snappy import ProductIO

import subprocess
import time

from lxml import etree
from shapely import wkb
from dbio import *

image_path = os.getenv('IMAGES_PATH')
slave_path = os.getenv('SLAVES_PATH')
intermediate_output_path = os.getenv('INTERMEDIATE_OUTPUT_PATH')
output_path = os.getenv('OUTPUT_PATH')

graph_path = os.getenv('PROCESSING_XML')
graph_wo_coregister_path = os.getenv('PROCESSING_WO_COREGISTRATION_XML')
coregister_path = os.getenv('COREGISTRATION_XML')
coregister_template = os.getenv('COREGISTRATION_TEMPLATE')

subset_path = os.getenv('SUBSET_TEMPLATE')
subset_template = os.getenv('SUBSET_XML')

gpt_path = os.getenv('GPT_PATH')

exec_cmd = '%s %s -t %s{}_Orb_TNR_Bdr_Cal_ML_TF.dim {}.zip' % (gpt_path, graph_path, intermediate_output_path)
coreg_cmd = '%s %s -t %s{}_Orb_TNR_Bdr_Cal_ML_TF_Stack_Spk_TC.dim' % (gpt_path, coregister_path, intermediate_output_path)

exec_wo_coregister_cmd = '%s %s -t %s{}_Orb_TNR_Bdr_Cal_ML_TF_Spk_TC.dim {}.zip' % (gpt_path, graph_wo_coregister_path, intermediate_output_path)

export_cmd = '%s %s -t %s{}_{} -f GeoTIFF-BigTIFF' % (gpt_path, subset_path, output_path)


def parse(source):
    try:
        parser = etree.XMLParser(
            huge_tree=True,
            no_network=False,
            remove_blank_text=True,
        )
        return etree.parse(source, parser)
    except Exception as e:
        print("XML Parse error: {}".format(e))
        return None


def tostring(tree, xml_declaration=False, pretty_print=True):
    return etree.tostring(
        tree,
        xml_declaration=xml_declaration,
        encoding="utf-8",
        pretty_print=pretty_print,
    ).decode("utf-8'")


def set_path():
    os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(os.getcwd())
    os.chdir(path)
    return path


def get_unprocessed_data(year, table_name):
    query = "SELECT title, footprint FROM {} WHERE beginposition >= '{}-01-01' AND beginposition < '{}-01-01' " \
            "AND processed={} AND slave={} ORDER BY title;".format(table_name, year, year + 1, False, False)
    data = get_query(query)
    output = []
    for _data in data:
        pair = {
            'geom': wkb.loads(_data[1], hex=True),
            'file': '{}{}'.format(image_path, _data[0]),
            'title': '{}'.format(_data[0])
        }
        output.append(pair)
    return output


def get_slaves(table_name):
    query = "SELECT title, footprint FROM {} WHERE slave={};".format(table_name, True)
    data = get_query(query)
    output = []
    for _data in data:
        pair = {
            'geom': wkb.loads(_data[1], hex=True),
            'title': _data[0]
        }
        output.append(pair)
    return output


def get_intersecting_slaves(master, slaves):
    output = [{
        'geom': master['geom'],
        'file': '{}{}_Orb_TNR_Bdr_Cal_ML_TF.dim'.format(intermediate_output_path, master['title'])
    }]
    for slave in slaves:
        if slave['geom'].intersects(master['geom']):
            pair = {
                'geom': slave['geom'],
                # salves has old naming convections. Update it if needed
                'file': '{}{}_Orb_Cal_ML_TF.dim'.format(slave_path, slave['title'])
            }
            output.append(pair)
    return output


def get_nodes():
    registration_tree = parse(coregister_template)
    master_slave_node = registration_tree.findall("./node[@id='MasterSlaveReaderNode']")[0]
    terrain_correction_node = registration_tree.findall("./node[@id='TerrainCorrectionNode']")[0]
    return registration_tree, master_slave_node, terrain_correction_node


def get_subset_node():
    subset_tree = parse(subset_template)
    product_reader_node = subset_tree.findall("./node[@id='ProductReaderNode']")[0]
    subset_node = subset_tree.findall("./node[@id='SubsetNode']")[0]
    return subset_tree, product_reader_node, subset_node


def list_dict_to_string(list, property):
    files = []
    for item in list:
        files.append(item[property])
    return ','.join(files)


def get_string_month(month):
    month_map = {
        '01': 'Jan',
        '02': 'Feb',
        '03': 'Mar',
        '04': 'Apr',
        '05': 'May',
        '06': 'Jun',
        '07': 'Jul',
        '08': 'Aug',
        '09': 'Sep',
        '10': 'Oct',
        '11': 'Nov',
        '12': 'Dec'
    }
    return month_map[month]


def main():
    table_name = os.getenv('TABLE_NAME')
    slave_table = os.getenv('SLAVE_TABLE')

    year = int(os.getenv('YEAR'))
    data = get_unprocessed_data(year, table_name)
    slaves = get_slaves(slave_table)

    for _data in data:
        intersecting_slaves = get_intersecting_slaves(_data, slaves)
        try:
            start_time = time.time()
            file_name = _data['file'].split(image_path)[1]
            print('********************************************************')
            print('started file: {}'.format(file_name))

            image_datetime = file_name.split('_')[4]
            image_year = image_datetime[0:4]
            image_month = image_datetime[4:6]
            image_month_string = get_string_month(image_month)
            image_day = image_datetime[6:8]
            band_name_template = 'Gamma0_{}_mst_' + image_day + image_month_string + image_year

            product = ProductIO.readProduct('{}.zip'.format(_data['file']))
            bands = list(product.getBandNames())
            _select_bands = []
            for band in bands:
                if 'VV' in band:
                    _select_bands.append(band_name_template.format('VV'))
                elif 'VH' in band:
                    _select_bands.append(band_name_template.format('VH'))
                elif 'HH' in band:
                    _select_bands.append(band_name_template.format('HH'))
                elif 'HV' in band:
                    _select_bands.append(band_name_template.format('HV'))

            select_bands = list(set(_select_bands))

            if len(intersecting_slaves) - 1 < 2:
                # without coregistration
                logging.info('> processing {} without coregistration because no interesting slaves found'.format(_data['file']))
                processing = exec_wo_coregister_cmd.format(file_name, _data['file'])
                result = subprocess.check_output(processing, shell=True)
                print(result)
            else:
                logging.info('> processing {} with {} slaves'.format(_data['file']), len(intersecting_slaves) - 1)
                before_coregistration = exec_cmd.format(file_name, _data['file'])
                result = subprocess.check_output(before_coregistration, shell=True)
                print(result)

                registration_tree, master_slave_node, terrain_correction_node = get_nodes()
                master_slave_list = master_slave_node.xpath('//fileList')[0]
                master_slave_list.text = list_dict_to_string(intersecting_slaves, 'file')

                source_bands_list = terrain_correction_node.xpath('//sourceBands')[0]
                source_bands_list.text = ','.join(select_bands)

                with open(coregister_path, 'w') as f:
                    f.write(tostring(registration_tree, pretty_print=True))

                after_coregistration = coreg_cmd.format(file_name)
                result = subprocess.check_output(after_coregistration, shell=True)
                print(result)

            for select_band in select_bands:
                subset_tree, product_reader_node, subset_node = get_subset_node()
                file_source = product_reader_node.xpath('//file')[0]
                if len(intersecting_slaves) - 1 < 2:
                    file_source.text = '{}{}_Orb_TNR_Bdr_Cal_ML_TF_Spk_TC.dim'.format(intermediate_output_path, file_name)
                else:
                    file_source.text = '{}{}_Orb_TNR_Bdr_Cal_ML_TF_Stack_Spk_TC.dim'.format(intermediate_output_path, file_name)
                subset_source_band = subset_node.xpath('//sourceBands')[0]
                subset_source_band.text = select_band
                with open(subset_path, 'w') as f:
                    f.write(tostring(subset_tree, pretty_print=True))

                subsetting = export_cmd.format(file_name, select_band)
                result = subprocess.check_output(subsetting, shell=True)
                print(result)

                os.remove(subset_path)

            conn, cur = connect_to_db(db)
            cur.execute("UPDATE {} SET processed=TRUE WHERE slave=FALSE and title='{}'".format(table_name, file_name))
            conn.commit()
            close_connection(conn, cur)

            if os.path.exists(coregister_path):
                os.remove(coregister_path)

            end_time = time.time()

            with open('logs/performance.log', 'a') as plog:
                plog.write('> file {} => {} slaves => {} minutes\n'.format(file_name, len(intersecting_slaves) - 1,
                                                                         (end_time - start_time) / 60))

            print('end file')
            print('********************************************************')
        except Exception as e:
            logging.error('> total {} slaves \n error {}'.format(len(intersecting_slaves) - 1, e))
            print(e)
            continue


if __name__ == '__main__':
    main()
