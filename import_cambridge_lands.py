import itertools
import csv
import os

import requests
import click

from importers import CSVImportCommand
from utils import transform_polygons_to_multipolygon
import hmrc_addressbase

class CambridgeLandsImportCommand(CSVImportCommand):
    def __init__(
            self, file_names, api_url, token,
            lr_api_url, lr_token,
            voa_api_url, voa_token,
            skip_header=True, encoding=None,
            filter_uprn=None, vacant_csv_filename=None,
            ):
        self.api_url = api_url
        self.token = token
        self.lr_api_url = lr_api_url
        self.lr_token = lr_token
        self.voa_api_url = voa_api_url
        self.voa_token = voa_token
        self.file_names = file_names
        self.skip_header = skip_header
        self.encoding = encoding
        self.filter_uprn = filter_uprn
        if vacant_csv_filename:
            self.vacant_csv_filename = vacant_csv_filename
            self.vacant_csv_file = open(vacant_csv_filename, 'w', newline='')
            self.vacant_header = VACANT_CSV_HEADER
            self.vacant_csv = csv.DictWriter(
                self.vacant_csv_file, fieldnames=self.vacant_header,
                dialect='excel')
            self.vacant_csv.writeheader()
        assert self.lr_api_url.endswith('/api/')

    def get_lr_data(self, uprn):
        url = '{0}uprns/{1}/'.format(self.lr_api_url, uprn)
        headers = {'Authorization': 'Token {0}'.format(self.lr_token)}
        response = requests.get(url, headers=headers)

        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def get_lr_polygons_from_point(self, lat, long):
        url = '{url}polygons-from-point?lat={lat}&long={long}'.format(
            url=self.lr_api_url, lat=lat, long=long)
        headers = {'Authorization': 'Token {0}'.format(self.lr_token)}
        response = requests.get(url, headers=headers)

        response.raise_for_status()
        return response.json()

    def get_voa_data(self, ba_ref):
        url = '{0}{1}'.format(self.voa_api_url, ba_ref)
        headers = {'Authorization': 'Token {0}'.format(self.voa_token)}
        response = requests.get(url, headers=headers)

        if response.status_code == 404:
            return None
        response.raise_for_status()
        return response.json()

    def process_row(self, row):
        uprn = row[3]

        if self.filter_uprn and uprn != self.filter_uprn:
            return 'Filtered out by uprn'

        if not (row[14] == 'EPRN' or row[14] == 'EPRI' or row[13] == 'VOID'):
            return 'Ignore - not vacant'

        def format_uprn_for_csv(uprn):
            return "'{}".format(int(uprn)) if uprn else ''
        def lookup_uprn_in_lr(uprn):
            try:
                lr_data = self.get_lr_data(uprn)
            except Exception as e:
                site_info['uprn_status'] = 'Error - contacting LR server'
                raise FinishedProcessingRow(
                    'Error accessing LR server: {}'.format(
                    repr(e).replace(uprn, '<UPRN>')))
            if lr_data:
                site_info['uprn_status'] = 'ok'
            return lr_data

        try:
            site_info = dict(
                name=row[6],
                address=row[7],
                ba_ref=row[4],
                uprn=format_uprn_for_csv(row[3]),
            )

            if (uprn in ('', '0')):
                site_info['uprn_status'] = 'Error - none'
                return 'Error - no uprn'
            try:
                lr_data = lookup_uprn_in_lr(uprn)
            except FinishedProcessingRow as ex:
                return str(ex)
        finally:
            if self.vacant_csv:
                addressbase_response = \
                    hmrc_addressbase.lookup_uprn_in_addressbase(uprn)
                if addressbase_response:
                    if len(addressbase_response) > 1:
                        print('Warning too many addresses for single uprn')
                    site_info['address_from_addressbase_uprn'] = \
                        hmrc_addressbase.serialize_address(
                            addressbase_response[0]['address'])
                    lat_long = addressbase_response[0]['location']
                    site_info['latlong_from_addressbase_uprn'] = lat_long
                    # lookup lat/long in LR data
                    results = self.get_lr_polygons_from_point(
                        lat=lat_long[0], long=lat_long[1])
                    titles = [
                        result['title']
                        for result in results
                        if result['title']]
                    uprns = list(itertools.chain.from_iterable([
                        result['uprns']
                        for result in results]))
                    site_info['num_polygons_from_latlong_addressbase_uprn'] = \
                        len(results)
                    polygons = [result['polygon'] for result in results]
                    site_info['num_unique_polygons_from_latlong_addressbase_uprn'] = \
                        len(set(polygons))
                    # site_info['polygon_from_latlong_addressbase_uprn'] = \
                    #     [result['polygon'] for result in results]
                    site_info['num_titles_from_latlong_addressbase_uprn'] = \
                        len(titles)
                    site_info['titles_from_latlong_addressbase_uprn'] = \
                        titles
                    site_info['num_uprns_from_latlong_addressbase_uprn'] = \
                        len(uprns)
                    site_info['uprns_from_latlong_addressbase_uprn'] = \
                        uprns
                    if site_info.get('uprn_status') != 'ok':
                        if len(uprns) > 0:
                            # we have derived the UPRN that Land Registry DOES
                            # have with a polygon in the same place.
                            uprn = uprns[0]
                            site_info['assumed_uprn'] = \
                                format_uprn_for_csv(uprn)
                            try:
                                lr_data = lookup_uprn_in_lr(uprn)
                            except FinishedProcessingRow as ex:
                                return str(ex) + ' (assumed uprn)'
                self.write_vacant_csv_row(site_info)
        if not lr_data:
            # We can't find polygons without a matching uprn, therefore
            # discard
            site_info['uprn_status'] = 'Error - LR has no matching title'
            return 'Error - no LR data for uprn'

        ba_ref = row[4]
        try:
            voa_data = self.get_voa_data(ba_ref)
        except Exception as e:
            # URL incorrect? Internet down? Do not fail silently
            return 'Error accessing VOA server: {}'.format(e)

        if voa_data:
            estimated_floor_space = voa_data.get('total_area')
            voa_status = 'with voa data'
        else:
            estimated_floor_space = 0
            voa_status = 'without voa data'

        data = {
            "uprn": uprn,
            "ba_ref": ba_ref,
            "name": row[6],
            "geom": transform_polygons_to_multipolygon(
                list(itertools.chain.from_iterable(
                    title['polygons'] for title in lr_data['titles']
                    ))
            ),
            "authority": row[0],
            "owner": 'Cambridge',
            "estimated_floor_space": estimated_floor_space,
            "srid": 4326
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=data,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(uprn))
            return 'processed ' + voa_status
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    uprn, response.text))
            return 'Error saving data to API: {} {}'.format(
                response.status_code, response.text)

    def write_vacant_csv_row(self, site_dict):
        self.vacant_csv.writerow(site_dict)

    def postprocess(self):
        if self.vacant_csv_file:
            self.vacant_csv_file.close()
            print('Written {}'.format(self.vacant_csv_filename))


VACANT_CSV_HEADER = [
    'name', 'address', 'ba_ref', 'uprn', 'uprn_status',
    'address_from_addressbase_uprn',
    'latlong_from_addressbase_uprn',
    'num_polygons_from_latlong_addressbase_uprn',
    'num_unique_polygons_from_latlong_addressbase_uprn',
    #'polygon_from_latlong_addressbase_uprn',
    'num_titles_from_latlong_addressbase_uprn',
    'titles_from_latlong_addressbase_uprn',
    'num_uprns_from_latlong_addressbase_uprn',
    'uprns_from_latlong_addressbase_uprn',
    'assumed_uprn',
    ]

class FinishedProcessingRow(Exception):
    pass

@click.command()
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/locations/', help='API url')
@click.option('--apitoken', help='API authentication token')
@click.option(
    '--lrapiurl',
    default='http://localhost:8001/api/',
    help='Land Registry API base url')
@click.option('--lrtoken', help='Land Registry API authentication token')
@click.option(
    '--voaapiurl',
    default='http://localhost:8002/api/voa/',
    help='VOA API url')
@click.option('--voatoken', help='VOA API authentication token')
@click.option('-u', '--filter-uprn', help='Filter rows for a particular UPRN')
@click.option('--vacant-csv', metavar='FILENAME',
              help='Output the vacant sites to a CSV')
def import_cambridge(
        filenames, apiurl, apitoken, lrapiurl, lrtoken, voaapiurl, voatoken,
        filter_uprn, vacant_csv):
    '''Import Cambridge vacant properties data as Locations.

    1. Get data from:
            https://www.cambridge.gov.uk/open-data
       titled "NDR accounts".
       e.g. https://www.cambridge.gov.uk/sites/default/files/nndr_accounts_2017-04.xlsx

       According to the council:
       > You can find the date declared vacant in column F. A property is
       > vacant if it states EPRN or EPRI in column O, or if it states VOID in
       > column N.

    2. Open in Excel and export as CSV

    3. Run this import
    '''
    command = CambridgeLandsImportCommand(
        filenames, apiurl, apitoken, lrapiurl, lrtoken, voaapiurl, voatoken,
        vacant_csv_filename=vacant_csv, filter_uprn=filter_uprn)
    command.run()


if __name__ == '__main__':
    import_cambridge()
