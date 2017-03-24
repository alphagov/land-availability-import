from importers import CSVImportCommand
import requests
import click


class SchoolsImportCommand(CSVImportCommand):

    def process_row(self, row):
        # Only import schools with easting and northing information
        if row[68] and row[69]:
            data = {
                "urn": row[0],
                "la_name": row[2],
                "school_name": row[4],
                "school_type": row[11],
                "school_capacity": int(row[20]) if row[20] else 0,
                "school_pupils": int(row[23]) if row[23] else 0,
                "postcode": row[44].replace(' ', ''),
                "point": {
                    "type": "Point",
                    "coordinates": [float(row[68]), float(row[69])]
                },
                "srid": 27700
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
@click.option('--filename', help='Schools *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/schools/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_schools(filename, apiurl, apitoken):
    command = SchoolsImportCommand(
        filename, apiurl, apitoken, True, encoding='ISO-8859-1')
    command.run()

if __name__ == '__main__':
    import_schools()
