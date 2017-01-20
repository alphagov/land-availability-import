from importers import CSVImportCommand
import requests
import click


class MetroTubeImportCommand(CSVImportCommand):

    def process_row(self, row):
        if row[31] == 'TMU':
            # import ipdb; ipdb.set_trace()
            data = {
                "atco_code": row[0],
                "name": row[4],
                "naptan_code": row[1],
                "locality": row[18],
                "point": {
                    "type": "Point",
                    "coordinates": [float(row[29]), float(row[30])]
                },
                "srid": 4326
            }

            headers = {'Authorization': 'Token {0}'.format(self.token)}

            response = requests.post(
                self.api_url,
                json=data,
                headers=headers)

            if response.status_code == 201:
                print('{0} imported correctly'.format(row[0]))
            else:
                print(
                    'ERROR: could not import {0} because of {1}'.format(
                        row[0], response.text))


@click.command()
@click.option('--filename', help='Metro and Tube *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/metrotube/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_metrotubes(filename, apiurl, apitoken):
    command = MetroTubeImportCommand(
        filename, apiurl, apitoken, True, encoding='ISO-8859-1')
    command.run()

if __name__ == '__main__':
    import_metrotubes()
