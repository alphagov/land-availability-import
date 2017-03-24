from importers import CSVImportCommand
import requests
import click


class CodepointImportCommand(CSVImportCommand):

    def process_row(self, row):
        postcode = row[0].strip().replace(' ', '').upper()

        data = {
            "postcode": postcode,
            "quality": row[1],
            "country": row[4],
            "nhs_region": row[5],
            "nhs_health_authority": row[6],
            "county": row[7],
            "district": row[8],
            "ward": row[9],
            "point": {
                "type": "Point",
                "coordinates": [float(row[2]), float(row[3])]
            },
            "srid": 27700
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=data,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(postcode))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    postcode, response.text))


@click.command()
@click.option('--filename', help='Codepoints *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/codepoints/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_codepoints(filename, apiurl, apitoken):
    command = CodepointImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_codepoints()
