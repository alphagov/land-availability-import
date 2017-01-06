from importers import CSVImportCommand
import requests
import click


class BusImportCommand(CSVImportCommand):

    def process_row(self, row):
        bus_stop = {
            "amic_code": row[0],
            "point": {
                "type": "Point",
                "coordinates": [float(row[2]), float(row[3])]
            },
            "name": row[4],
            "direction": row[5],
            "area": row[6],
            "road": row[7],
            "nptg_code": row[9],
            "srid": 27700
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
@click.option('--filename', help='Bus stops *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/busstop/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_busstops(filename, apiurl, apitoken):
    command = BusImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_busstops()
