from importers import ShapefileImportCommand
import requests
import click


class OverheadLinesImportCommand(ShapefileImportCommand):

    def process_record(self, record):
        data = {
            "gdo_gid": str(record.record[0]),
            "route_asset": str(record.record[1]),
            "towers": str(record.record[2]),
            "action_dtt": str(record.record[3]),
            "status": str(record.record[4]),
            "operating": str(record.record[5]),
            "circuit_1": str(record.record[6]),
            "circuit_2": str(record.record[7]),
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
@click.option('--filename', help='OverheadLines *.shp file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/overheadlines/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_overheadlines(filename, apiurl, apitoken):
    command = OverheadLinesImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_overheadlines()
