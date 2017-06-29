from importers import CSVImportCommand
import requests
import click


class BusImportCommand(CSVImportCommand):

    def process_row(self, row):
        bus_stop = {
            "amic_code": row[0],
            "point": {
                "type": "Point",
                "coordinates": [float(row[29]), float(row[30])]
            },
            "name": row[4],
            "direction": row[14],
            "area": row[19],
            "road": row[10],
            "nptg_code": row[1],
            "srid": 4326
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=bus_stop,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(row[0]))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    row[0], response.text))


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/busstops/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_busstops(filenames, apiurl, apitoken):
    command = BusImportCommand(filenames, apiurl, apitoken, True)
    command.run()

if __name__ == '__main__':
    import_busstops()
