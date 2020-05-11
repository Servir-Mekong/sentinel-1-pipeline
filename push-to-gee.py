# -*- coding: utf-8 -*-

from dotenv import load_dotenv
load_dotenv('.env')

import logging
logging.basicConfig(filename='logs/push-2-gee.log', level=logging.INFO)

import ast
import glob
import json
import os
import subprocess

from datetime import datetime
from dbio import *

scale_factor = 10000

output_path = os.getenv('OUTPUT_PATH')
final_output = os.getenv('POST_PROCESS_OUTPUT_PATH')
gdal_path = os.getenv('GDAL_PATH')
manifest_dir = os.getenv('MANIFESTS_PATH')
cloud_path = os.getenv('GCS_PATH')
gee_asset_path = os.getenv('GEE_ASSET_PATH')

calc = '{0}gdal_calc.py -A %s --calc="A*{1}" --outfile={2}%s --type=UInt16'.format(gdal_path, scale_factor, final_output)
_cp_to_gs = 'gsutil cp {0}%s {1}'.format(final_output, cloud_path)
_upload_to_gee = 'earthengine upload image --manifest "{0}%s.json"'.format(manifest_dir)

properties = ['acquisitiontype', 'lastorbitnumber', 'lastrelativeorbitnumber', 'missiondatatakeid', 'orbitdirection',
              'orbitnumber', 'platformidentifier', 'polarisationmode', 'producttype', 'relativeorbitnumber',
              'sensoroperationalmode', 'swathidentifier']


def get_processed_images(table_name):
    query = "SELECT id, title, beginposition, endposition, {} FROM {} WHERE processed={} AND slave={} " \
            "AND uploadedtogs={} ORDER BY title;".format(','.join(properties), table_name, True, False, False)
    return get_query(query)


def main():
    table_name = os.getenv('TABLE_NAME')
    processed_images = get_processed_images(table_name)
    for image in processed_images:
        output_files = glob.glob(output_path + '*{}*.tif'.format(image[1]))

        # push to gee
        # name
        manifest_name = '{}/{}'.format(gee_asset_path, image[1])
        # tilesets and bands
        manifest_tilesets = []
        manifest_bands = []
        for index, output in enumerate(output_files):
            file_name = output.split(output_path)[1]

            # convert to uint 16
            calc_command = calc % (output, file_name)
            result = subprocess.check_output(calc_command, shell=True)
            print(result)
            # push to gs
            cp_to_gs = _cp_to_gs % (file_name)
            result = subprocess.check_output(cp_to_gs, shell=True)
            print(result)

            if 'VH' in file_name:
                tileset_id = 'tileset_for_band_VH_{}'.format(index)
                band_id = 'VH'
            elif 'VV' in file_name:
                tileset_id = 'tileset_for_band_VV_{}'.format(index)
                band_id = 'VV'

            _tileset = {
                'id': tileset_id,
                'sources': [
                    {
                        'uris': [
                            '{}/{}'.format(cloud_path, file_name)
                        ]
                    }
                ]
            }
            manifest_tilesets.append(_tileset)

            _band = {
                'id': band_id,
                'tileset_id': tileset_id
            }
            manifest_bands.append(_band)

        # properties
        manifest_properties = {}
        start_index = 4
        for _property in properties:
            manifest_properties[_property] = image[start_index]
            start_index += 1
        manifest_properties['scale_factor'] = scale_factor

        # start time
        manifest_start_time = {
            'seconds': int(round((image[2] - datetime(1970, 1, 1)).total_seconds()))
        }

        # end time
        manifest_end_time = {
            'seconds': int(round((image[3] - datetime(1970, 1, 1)).total_seconds()))
        }

        final_manifest = {
            'name': manifest_name,
            'tilesets': manifest_tilesets,
            'bands': manifest_bands,
            'start_time': manifest_start_time,
            'end_time': manifest_end_time,
            'properties': manifest_properties
        }

        with open('{}{}.json'.format(manifest_dir, image[1]), 'w') as manifest_file:
            json.dump(ast.literal_eval(json.dumps(final_manifest)), manifest_file, ensure_ascii=False, indent=4)

        upload_to_gee = _upload_to_gee % (image[1])
        result = subprocess.check_output(upload_to_gee, shell=True)
        print(result)

        if 'ID:' in result:
            task_id = result.split("ID:")[1].strip()
            # save the info
            query = "UPDATE {} SET uploadedtogs=TRUE, ee_task_id='{}' WHERE id='{}'".format(table_name, task_id, image[0])
            update_query(query)


if __name__ == '__main__':
    main()
