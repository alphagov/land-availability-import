from importers import CSVImportCommand
import requests
import click


class UprnsImportCommand(CSVImportCommand):

    def process_row(self, row):
        uprn = row[1]
        title = row[0]

        data = {
            "uprn": uprn,
            "title": title
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
                    uprn, response.text))


@click.command()
@click.option('--filename', help='UPRNs *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/uprns/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_uprns(filename, apiurl, apitoken):
    command = UprnsImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_uprns()
