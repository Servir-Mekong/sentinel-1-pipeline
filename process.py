import logging
logging.basicConfig(filename='process.access.log', level=logging.INFO)

import os
import psycopg2
import subprocess
import time
from lxml import etree
from shapely import wkb
import sys
sys.path.append('/mnt/sentinel1')
from snappy import ProductIO

image_path = '/mnt/sentinel1/images/'
slave_path = '/mnt/sentinel1/slaves/'
intermediate_output_path = '/mnt/sentinel1/intermediate_output/'
output_path = '/mnt/sentinel1/output/'

graph_path = '/mnt/sentinel1/pipeline.xml'
coregister_path = '/mnt/sentinel1/coregister.xml'
coregister_template = '/mnt/sentinel1/coregister_template.xml'

subset_path = '/mnt/sentinel1/subset.xml'
subset_template = '/mnt/sentinel1/subset_template.xml'

gpt_path = '/usr/local/snap/bin/gpt'

exec_cmd = '%s %s -t %s{}_Orb_Cal_ML_TF.dim {}.zip' % (gpt_path, graph_path, intermediate_output_path)
coreg_cmd = '%s %s -t %s{0}_Orb_Cal_ML_TF_Stack_Spk_EC.dim' % (gpt_path, coregister_path, intermediate_output_path)
export_cmd = '%s %s -t %s{}_{} -f GeoTIFF-BigTIFF' % (gpt_path, subset_path, output_path)


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
    else:
        return conn, conn.cursor()

def close_connection(cur, conn):
    cur.close()
    conn.close()

def parse(source):

    try:
        parser = etree.XMLParser(
            huge_tree = True,
            no_network = False,
            remove_blank_text = True,
        )
        return etree.parse(source, parser)
    except Exception as e:
        print("XML Parse error: {}".format(e))
        return None

def tostring(tree, xml_declaration=False, pretty_print=True):

    return etree.tostring(
        tree,
        xml_declaration = xml_declaration,
        encoding = "utf-8",
        pretty_print = pretty_print,
    ).decode("utf-8'")

def set_path():
    os.path.dirname(os.path.dirname(__file__))
    path = os.path.join(os.getcwd())
    os.chdir(path)
    return path

def get_unprocessed_data(year):
    conn, cur = connect_to_db(db)
    cur.execute("SELECT title, footprint FROM sentinel1 WHERE beginposition >= '{}-01-01' AND beginposition < '{}-01-01' AND processed={} AND slave={} ORDER BY title;".format(year, year+1, False, False))
    data = cur.fetchall()
    close_connection(conn, cur)
    output = []
    for _data in data:
        pair = {
            'geom' : wkb.loads(_data[1], hex=True),
            'file': '{}{}'.format(image_path, _data[0]),
            'title': '{}'.format(_data[0])
        }
        output.append(pair)
    return output

def get_slaves():
    conn, cur = connect_to_db(db)
    cur.execute("SELECT title, footprint FROM sentinel1 WHERE slave={};".format(True,))
    data = cur.fetchall()
    close_connection(conn, cur)
    output = []
    for _data in data:
        pair = {
            'geom' : wkb.loads(_data[1], hex=True),
            'title': _data[0]
        }
        output.append(pair)
    return output

def get_intersecting_slaves(master, slaves):
    output = []
    output.append({
        'geom': master['geom'],
        'file': '{}{}_Orb_Cal_ML_TF.dim'.format(intermediate_output_path, master['title'])
    })
    for slave in slaves:
        if (slave['geom'].intersects(master['geom'])):
            pair = {
                'geom' : slave['geom'],
                'file':  '{}{}_Orb_Cal_ML_TF.dim'.format(slave_path, slave['title'])
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

    year = 2018

    data = get_unprocessed_data(year)

    slaves = get_slaves()

    for _data in data:
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

            intersecting_slaves = get_intersecting_slaves(_data, slaves)
            before_coregistration = exec_cmd.format(file_name, _data['file'])
            result = subprocess.check_output(before_coregistration, shell=True)
            print(result)

            registration_tree, master_slave_node, terrain_correction_node = get_nodes()
            master_slave_list = master_slave_node.xpath('//fileList')[0]
            master_slave_list.text = list_dict_to_string(intersecting_slaves, 'file')

            source_bands_list = terrain_correction_node.xpath('//sourceBands')[0]
            source_bands_list.text =  ','.join(select_bands)

            with open(coregister_path, 'w') as f:
                f.write(tostring(registration_tree, pretty_print=True))

            after_coregistration = coreg_cmd.format(file_name)
            result = subprocess.check_output(after_coregistration, shell=True)
            print(result)

            for select_band in select_bands:
                subset_tree, product_reader_node, subset_node = get_subset_node()
                file_source = product_reader_node.xpath('//file')[0]
                file_source.text = '{}{}_Orb_Cal_ML_TF_Stack_Spk_EC.dim'.format(intermediate_output_path, file_name)
                subset_source_band = subset_node.xpath('//sourceBands')[0]
                subset_source_band.text = select_band
                with open(subset_path, 'w') as f:
                    f.write(tostring(subset_tree, pretty_print=True))

                subsetting = export_cmd.format(file_name, select_band)
                result = subprocess.check_output(subsetting, shell=True)
                print(result)

                os.remove(subset_path)


            conn, cur = connect_to_db(db)
            cur.execute("UPDATE sentinel1 SET processed=TRUE WHERE slave=FALSE and title='{}'".format(file_name))
            conn.commit()
            close_connection(conn, cur)

            os.remove(coregister_path)
            
            end_time = time.time()

            with open('/mnt/sentinel1/performance.log', 'a') as plog:
                plog.write('file {} => {} slaves => {} minutes\n'.format(file_name, len(intersecting_slaves) -1, (end_time - start_time)/60))

            print('end file')
            print('********************************************************')
        except Exception as e:
            logging.error(e)
            print(e)
            continue

if __name__ == '__main__':
    main()
