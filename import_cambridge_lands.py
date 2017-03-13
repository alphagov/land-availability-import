from importers import CSVImportCommand
import requests
import click


class CambridgeLandsImportCommand(CSVImportCommand):
    def __init__(
            self, file_name, api_url, token,
            lr_api_url, lr_token,
            skip_header=True, encoding=None):
        self.api_url = api_url
        self.token = token
        self.lr_api_url = lr_api_url
        self.lr_token = lr_token
        self.file_name = file_name
        self.skip_header = skip_header
        self.encoding = encoding

        if not self.lr_api_url.endswith('/'):
            self.lr_api_url = self.lr_api_url + '/'

    def get_lr_data(self, uprn):
        url = '{0}{1}'.format(self.lr_api_url, uprn)
        headers = {'Authorization': 'Token {0}'.format(self.lr_token)}
        response = requests.get(url, headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            return None

    def process_row(self, row):
        uprn = row[3]

        if (uprn != '' and uprn != '0'):
            if (row[14] == 'EPRN' or row[14] == 'EPRI' or row[13] == 'VOID'):
                lr_data = self.get_lr_data(uprn)

                data = {
                    "uprn": uprn,
                    "ba_ref": row[4],
                    "name": row[6],
                    "geom": lr_data['features'][0]['geometry'],
                    "authority": row[0],
                    "owner": '',
                    "srid": 4326
                }

                headers = {'Authorization': 'Token {0}'.format(self.token)}

                response = requests.post(
                    self.api_url,
                    json=data,
                    headers=headers)

                if response.status_code == 201:
                    print('{0} imported correctly'.format(uprn))
                else:
                    print(
                        'ERROR: could not import {0} because of {1}'.format(
                            uprn, response.text))


@click.command()
@click.option('--filename', help='Cambridge *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/locations/', help='API url')
@click.option('--apitoken', help='API authentication token')
@click.option(
    '--lrapiurl',
    default='http://localhost:8001/api/lr/',
    help='Land Registry API url')
@click.option('--lrtoken', help='Land Registry API authentication token')
def import_cambridge(filename, apiurl, apitoken, lrapiurl, lrtoken):
    command = CambridgeLandsImportCommand(
        filename, apiurl, apitoken, lrapiurl, lrtoken)
    command.run()

if __name__ == '__main__':
    import_cambridge()
