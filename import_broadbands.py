from importers import CSVImportCommand
import requests
import click


class BroadbandImportCommand(CSVImportCommand):

    def clean_column(self, column):
        clean = column.replace('<', '').replace('N/A', '')

        if clean == '':
            return '0'
        else:
            return clean

    def process_row(self, row):
        data = {
            "postcode": row[0],
            "speed_30_mb_percentage": float(self.clean_column(row[2])),
            "avg_download_speed": float(self.clean_column(row[7])),
            "min_download_speed": float(self.clean_column(row[9])),
            "max_download_speed": float(self.clean_column(row[10])),
            "avg_upload_speed": float(self.clean_column(row[15])),
            "min_upload_speed": float(self.clean_column(row[17])),
            "max_upload_speed": float(self.clean_column(row[18]))
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
@click.option('--filename', help='Broadbands *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/broadband/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_broadbands(filename, apiurl, apitoken):
    command = BroadbandImportCommand(filename, apiurl, apitoken, True)
    command.run()

if __name__ == '__main__':
    import_broadbands()
