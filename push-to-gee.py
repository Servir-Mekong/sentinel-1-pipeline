import logging
logging.basicConfig(filename='push-2-gee.log', level=logging.INFO)

import ast
import glob
import psycopg2
import subprocess
import json
from datetime import datetime

output_path = '/mnt/sentinel1/output/'
final_output = '/mnt/sentinel1/output-2-push/'
gdal_path = '/home/bbhandari/anaconda3/envs/gee/bin/'
manifest_dir = '/mnt/sentinel1/manifests/'

calc = '{0}gdal_calc.py -A %s --calc="A*10000" --outfile={1}%s --type=UInt16'.format(gdal_path, final_output)
_cp_to_gs = 'gsutil cp {0}%s gs://servirmekong/sentinel1'.format(final_output)
_upload_to_gee = 'earthengine upload image --manifest "{0}%s.json"'.format(manifest_dir)

db = {
    'dbname': '<your-db>',
    'user': '<your-username>',
    'host': 'localhost',
    'password': '<your-password>'
}

properties = ['acquisitiontype', 'lastorbitnumber', 'lastrelativeorbitnumber', 'missiondatatakeid', 'orbitdirection', 'orbitnumber',
              'platformidentifier', 'polarisationmode', 'producttype', 'relativeorbitnumber', 'sensoroperationalmode', 'swathidentifier']

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

def get_processed_images():
    conn, cur = connect_to_db(db)
    cur.execute("SELECT id, title, beginposition, endposition, {} \
                 FROM sentinel1 WHERE processed={} AND slave={} AND uploadedtogs={} ORDER BY title;".format(','.join(properties), True, False, False))
    data = cur.fetchall()
    close_connection(conn, cur)
    return data

def main():
    processed_images = get_processed_images()
    for image in processed_images:
        output_files = glob.glob(output_path + "*{}*.tif".format(image[1]))

        # push to gee
        # name
        manifest_name = "projects/earthengine-legacy/assets/projects/servir-mekong/sentinel1/{}".format(image[1])
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
                tileset_id = "tileset_for_band_VH_{}".format(index)
                band_id = "VH"
            elif 'VV' in file_name:
                tileset_id = "tileset_for_band_VV_{}".format(index)
                band_id = "VV"

            _tileset = {
                "id": tileset_id,
                "sources": [
                    {
                        "uris": [
                            "gs://servirmekong/sentinel1/{}".format(file_name)
                        ]
                    }
                ]
            }
            manifest_tilesets.append(_tileset)

            _band = {
                "id": band_id,
                "tileset_id": tileset_id
            }
            manifest_bands.append(_band)

        # properties
        manifest_properties = {}
        start_index = 4
        for property in properties:
            manifest_properties[property] = image[start_index]
            start_index += 1

        # start time
        manifest_start_time = {
            "seconds": int(round((image[2] - datetime(1970, 1, 1)).total_seconds()))
        }

        # end time
        manifest_end_time = {
            "seconds": int(round((image[3] - datetime(1970, 1, 1)).total_seconds()))
        }

        final_manifest = {
            "name": manifest_name,
            "tilesets": manifest_tilesets,
            "bands": manifest_bands,
            "start_time": manifest_start_time,
            "end_time": manifest_end_time,
            "properties": manifest_properties
        }

        with open("{}{}.json".format(manifest_dir, image[1]), "w") as manifest_file:
            json.dump(ast.literal_eval(json.dumps(final_manifest)), manifest_file, ensure_ascii=False, indent=4)

        upload_to_gee = _upload_to_gee % (image[1])
        result = subprocess.check_output(upload_to_gee, shell=True)
        print(result)

        if "ID:" in result:
            task_id = result.split("ID:")[1].strip()
            # save the info
            conn, cur = connect_to_db(db)
            cur.execute("UPDATE sentinel1 SET uploadedtogs=TRUE, ee_task_id='{}' WHERE id='{}'".format(task_id, image[0]))
            conn.commit()
            close_connection(conn, cur)

if __name__ == '__main__':
    main()
