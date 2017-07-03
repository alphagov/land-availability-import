from importers import CSVImportCommand
import requests
import click


class BusImportCommand(CSVImportCommand):

    def process_row(self, row):
        try:
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
        except UnicodeDecodeError as ex:
            print(
                'ERROR: could not import {0} because of {1}'.format(row, ex))


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/busstops/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_busstops(filenames, apiurl, apitoken):
    expected_header = [
        'ATCOCode', 'NaptanCode', 'PlateCode', 'CleardownCode', 'CommonName',
        'CommonNameLang', 'ShortCommonName', 'ShortCommonNameLang',	'Landmark',
        'LandmarkLang',	'Street', 'StreetLang',	'Crossing',	'CrossingLang',
        'Indicator', 'IndicatorLang', 'Bearing', 'NptgLocalityCode',
        'LocalityName',	'ParentLocalityName', 'GrandParentLocalityName',
        'Town', 'TownLang',	'Suburb', 'SuburbLang',	'LocalityCentre',
        'GridType',	'Easting', 'Northing', 'Longitude',	'Latitude',	'StopType',
        'BusStopType', 'TimingStatus', 'DefaultWaitTime', 'Notes', 'NotesLang',
        'AdministrativeAreaCode', 'CreationDateTime', 'ModificationDateTime',
        'RevisionNumber', 'Modification', 'Status'
    ]
    command = BusImportCommand(
        filenames, apiurl, apitoken, True, expected_header=expected_header)
    command.run()

if __name__ == '__main__':
    import_busstops()
