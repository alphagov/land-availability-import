from importers import CSVImportCommand
import requests
import click


class AddressImportCommand(CSVImportCommand):

    def process_row(self, row):
        address = {
            "uprn": row[0],
            "address_line_1": row[2],
            "address_line_2": row[3],
            "address_line_3": row[4],
            "city": row[5],
            "county": row[6],
            "postcode": row[7],
            "country_code": row[9],
            "point": {
                "type": "Point",
                "coordinates": [float(row[10]), float(row[9])]
            },
            "srid": 4326
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=address,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(row[0]))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    row[0], response.text))


@click.command()
@click.option('--filename', help='Addresses *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/addresses/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_addresses(filename, apiurl, apitoken):
    command = AddressImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_addresses()
