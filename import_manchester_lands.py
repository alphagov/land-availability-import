import requests
import click
import json


class ManchesterLandsImportCommand(object):

    def __init__(self, file_name, api_url, token):
        self.api_url = api_url
        self.token = token
        self.file_name = file_name

    def run(self):
        if self.file_name:
            with open(self.file_name) as jsonfile:
                data = json.load(jsonfile)

            for feature in data['features']:
                self.process_feature(feature)

    def process_feature(self, feature):
        uprn = feature.get('uprn')
        geom_type = feature['geometry']['type']

        if uprn and (geom_type == 'MultiPolygon'):
            print('Importing: {0}'.format(uprn))

            data = {
                "uprn": uprn,
                "name": feature['properties']['address'],
                "geom": feature['geometry'],
                "authority": feature['properties']['la'],
                "owner": feature['properties']['la'],
                "srid": 3857
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
@click.option('--filename', help='Manchester buildings *.json file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/locations/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_lands(filename, apiurl, apitoken):
    command = ManchesterLandsImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_lands()
