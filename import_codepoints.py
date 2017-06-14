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
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/codepoints/', nargs=1, help='API url')
@click.option('--apitoken', nargs=1, help='API authentication token')
def import_codepoints(filenames, apiurl, apitoken):
    command = CodepointImportCommand(filenames, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_codepoints()
