from importers import ShapefileImportCommand
import requests
import click


class MotorwaysImportCommand(ShapefileImportCommand):

    def process_record(self, record):
        data = {
            "identifier": record.record[0],
            "number": record.record[1],
            "point": record.shape.__geo_interface__,
            "srid": 27700
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=data,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(record.record[0]))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    record.record[0], response.text))


@click.command()
@click.option('--filename', help='Motorways *.shp file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/motorway/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_motorways(filename, apiurl, apitoken):
    command = MotorwaysImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_motorways()
