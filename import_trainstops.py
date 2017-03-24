from importers import CSVImportCommand
import requests
import click


class TrainImportCommand(CSVImportCommand):

    def process_row(self, row):
        train_stop = {
            "atcode_code": row[0],
            "naptan_code": row[1],
            "point": {
                "type": "Point",
                "coordinates": [float(row[2]), float(row[3])]
            },
            "main_road": row[5],
            "side_road": row[6],
            "type": row[7],
            "nptg_code": row[8],
            "local_reference": row[9],
            "srid": 27700
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=train_stop,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(row[0]))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    row[0], response.text))


@click.command()
@click.option('--filename', help='Train stops *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/trainstops/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_trainstops(filename, apiurl, apitoken):
    command = TrainImportCommand(filename, apiurl, apitoken, True)
    command.run()

if __name__ == '__main__':
    import_trainstops()
