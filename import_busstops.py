from importers import CSVImportCommand
import requests
import click


class BusImportCommand(CSVImportCommand):

    def __init__(self, *kargs, **kwargs):
        self.dry_run = kwargs.pop('dry_run')
        super(BusImportCommand, self).__init__(
            *kargs, **kwargs)

    def process_row(self, row):
        # Import the record only if it's one of the possible bus stops
        if row[31] in ['BCT', 'BCE', 'BST', 'BCS', 'BCQ']:
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

            if self.dry_run:
                return 'didn\'t import - dry run'
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
@click.option('--dry-run', is_flag=True, help='Does everything up to but '
              'not including writing the data to the API')
def import_busstops(filenames, apiurl, apitoken, dry_run):
    expected_header = [
        'ATCOCode', 'NaptanCode', 'PlateCode', 'CleardownCode', 'CommonName',
        'CommonNameLang', 'ShortCommonName', 'ShortCommonNameLang', 'Landmark',
        'LandmarkLang', 'Street', 'StreetLang', 'Crossing', 'CrossingLang',
        'Indicator', 'IndicatorLang', 'Bearing', 'NptgLocalityCode',
        'LocalityName', 'ParentLocalityName', 'GrandParentLocalityName',
        'Town', 'TownLang', 'Suburb', 'SuburbLang', 'LocalityCentre',
        'GridType', 'Easting', 'Northing', 'Longitude', 'Latitude', 'StopType',
        'BusStopType', 'TimingStatus', 'DefaultWaitTime', 'Notes', 'NotesLang',
        'AdministrativeAreaCode', 'CreationDateTime', 'ModificationDateTime',
        'RevisionNumber', 'Modification', 'Status'
    ]
    command = BusImportCommand(
        filenames, apiurl, apitoken, True, expected_header=expected_header,
        encoding='latin1', dry_run=dry_run)
    command.run()


if __name__ == '__main__':
    import_busstops()
