# -*- coding: utf-8 -*-

from dotenv import load_dotenv
load_dotenv('.env')

import logging
logging.basicConfig(filename='logs/metadata.download.log', level=logging.WARNING)

from datetime import datetime, timedelta
from dbio import *
import os
from sentinelsat import SentinelAPI, read_geojson, geojson_to_wkt

if __name__ == '__main__':
    start_year = int(os.getenv('YEAR'))
    _date = datetime(start_year, 1, 1)
    condition = True
    while condition:
        try:
            api = SentinelAPI(os.getenv('COPERNICUS_USERNAME'), os.getenv('COPERNICUS_PASSWORD'), 'https://scihub.copernicus.eu/dhus')
            footprint = geojson_to_wkt(read_geojson(os.getenv('AOI')))
            products = api.query(
                footprint,
                date=(_date.strftime('%Y%m%d'), (_date + timedelta(days=1)).strftime('%Y%m%d')),
                platformname='Sentinel-1',
                producttype='GRD'
            )
        except Exception as e:
            print('{} for date: {}'.format(e, _date.strftime('%Y-%m-%d')))
            logging.error('{} for date: {}'.format(e, _date.strftime('%Y-%m-%d')))
        else:
            fc = api.to_geojson(products)
            features = fc['features']
            if len(features):
                for feature in features:
                    properties = feature.properties
                    id = properties['id']
                    identifier = properties['identifier']
                    title = properties['title']
                    footprint = str(feature['geometry'])
                    acquisitiontype = properties['acquisitiontype']
                    beginposition = properties['beginposition']
                    endposition = properties['endposition']
                    filename = properties['filename']
                    format = properties['format']
                    instrumentname = properties['instrumentname']
                    instrumentshortname = properties['instrumentshortname']
                    lastorbitnumber = properties['lastorbitnumber']
                    lastrelativeorbitnumber = properties['lastrelativeorbitnumber']
                    quicklookiconname = properties['id']
                    missiondatatakeid = properties['missiondatatakeid']
                    orbitdirection = properties['orbitdirection']
                    orbitnumber = properties['orbitnumber']
                    platformidentifier = properties['platformidentifier']
                    polarisationmode = properties['polarisationmode']
                    productclass = properties['productclass']
                    producttype = properties['producttype']
                    relativeorbitnumber = properties['relativeorbitnumber']
                    sensoroperationalmode = properties['sensoroperationalmode']
                    size = properties['size']
                    slicenumber = properties['slicenumber']
                    swathidentifier = properties['swathidentifier']

                    conn, cur = connect_to_db()
                    try:
                        cur.execute(
                            "INSERT INTO sentinel1 (id, identifier, title, footprint, acquisitiontype, beginposition, endposition, filename, format, instrumentname, instrumentshortname, lastorbitnumber, lastrelativeorbitnumber, quicklookiconname, missiondatatakeid, orbitdirection, orbitnumber, platformidentifier, polarisationmode, productclass, producttype, relativeorbitnumber, sensoroperationalmode, size, slicenumber, swathidentifier) VALUES ('{}', '{}', '{}', ST_GeomFromGeoJSON('{}'), '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}');".format(
                                id, identifier, title, footprint, acquisitiontype, beginposition, endposition, filename,
                                format, instrumentname, instrumentshortname, lastorbitnumber, lastrelativeorbitnumber,
                                quicklookiconname, missiondatatakeid, orbitdirection, orbitnumber, platformidentifier,
                                polarisationmode, productclass, producttype, relativeorbitnumber, sensoroperationalmode,
                                size, slicenumber, swathidentifier))
                        conn.commit()
                    except Exception as e:
                        print('error with inserting because {}'.format(e))
                        logging.error('error with inserting because {}'.format(e))
                        continue
                    else:
                        close_connection(conn, cur)
            print('done date: {}'.format(e, _date.strftime('%Y-%m-%d')))

        _date += timedelta(days=1)
        if _date.year != start_year:
            condition = False
