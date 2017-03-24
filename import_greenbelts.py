import requests
import click
import json


class GreenbeltsImportCommand(object):

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
        if feature['geometry']['type'] == 'MultiPolygon':
            print('Importing: {0}'.format(feature['id']))

            data = {
                "code": feature["id"],
                "la_name": feature['properties']['LA_Name'],
                "gb_name": feature['properties']['GB_name'],
                "ons_code": feature['properties']['ONS_CODE'],
                "year": feature['properties']['Year'],
                "area": round(float(feature['properties']['Area_Ha']), 2),
                "perimeter": round(
                    float(feature['properties']['Perim_Km']), 2),
                "geom": feature['geometry'],
                "srid": 4326
            }

            headers = {'Authorization': 'Token {0}'.format(self.token)}

            response = requests.post(
                self.api_url,
                json=data,
                headers=headers)

            if response.status_code == 201:
                print('{0} imported correctly'.format(feature['id']))
            else:
                print(
                    'ERROR: could not import {0} because of {1}'.format(
                        feature['id'], response.text))


@click.command()
@click.option('--filename', help='Greenbelts *.json file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/greenbelts/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_greenbelts(filename, apiurl, apitoken):
    command = GreenbeltsImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_greenbelts()
