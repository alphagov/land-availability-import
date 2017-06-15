import itertools

import requests
import click

from importers import CSVImportCommand
from utils import transform_polygons_to_multipolygon


class CambridgeLandsImportCommand(CSVImportCommand):
    def __init__(
            self, file_names, api_url, token,
            lr_api_url, lr_token,
            voa_api_url, voa_token,
            skip_header=True, encoding=None,
            filter_uprn=None):
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

        if not self.lr_api_url.endswith('/'):
            self.lr_api_url = self.lr_api_url + '/'

    def get_lr_data(self, uprn):
        # some gazetteer database applications will pad UPRNs that are less
        # than 12 digits with zeros BUT the HMRC API needs you to remove them
        url = '{0}{1}'.format(self.lr_api_url, int(uprn))
        headers = {'Authorization': 'Token {0}'.format(self.lr_token)}
        response = requests.get(url, headers=headers)

        if response.status_code == 404:
            return None
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
        if (uprn in ('', '0')):
            return 'Error - no uprn'

        try:
            lr_data = self.get_lr_data(uprn)
        except Exception as e:
            return 'Error accessing LR server: {}'.format(
                repr(e).replace(uprn, '<UPRN>'))
        if not lr_data:
            # We can't find polygons without a matching uprn, therefore discard
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


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/locations/', help='API url')
@click.option('--apitoken', help='API authentication token')
@click.option(
    '--lrapiurl',
    default='http://localhost:8001/api/lr/',
    help='Land Registry API url')
@click.option('--lrtoken', help='Land Registry API authentication token')
@click.option(
    '--voaapiurl',
    default='http://localhost:8002/api/voa/',
    help='VOA API url')
@click.option('--voatoken', help='VOA API authentication token')
@click.option('-u', '--filter-uprn', help='Filter rows for a particular UPRN')
def import_cambridge(
        filenames, apiurl, apitoken, lrapiurl, lrtoken, voaapiurl, voatoken,
        filter_uprn):
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
        filter_uprn=filter_uprn)
    command.run()


if __name__ == '__main__':
    import_cambridge()
