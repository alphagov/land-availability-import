from importers import ShapefileImportCommand
import requests
import click


class SubstationsImportCommand(ShapefileImportCommand):

    def process_record(self, record):
        if record.shape.__geo_interface__['type'] == 'Polygon':
            data = {
                "name": str(record.record[0]),
                "operating": str(record.record[1]),
                "action_dtt": str(record.record[2]),
                "status": str(record.record[3]),
                "description": str(record.record[4]),
                "owner_flag": str(record.record[5]),
                "gdo_gid": str(record.record[6]),
                "geom": record.shape.__geo_interface__,
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
@click.option('--filename', help='Substations *.shp file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/substation/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_substations(filename, apiurl, apitoken):
    command = SubstationsImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_substations()
