from importers import CSVImportCommand
import requests
import click


class TrainImportCommand(CSVImportCommand):

    def process_row(self, row):
        try:
            if row[31] in ['RSE', 'RLY', 'RPL']:
                train_stop = {
                    "atcode_code": row[0],
                    "naptan_code": row[1],
                    "point": {
                        "type": "Point",
                        "coordinates": [float(row[29]), float(row[30])]
                    },
                    "main_road": row[10],
                    "side_road": '',
                    "type": row[8],
                    "nptg_code": row[17],
                    "local_reference": row[6],
                    "srid": 4326
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
        except UnicodeDecodeError as ex:
            print(
                'ERROR: could not import {0} because of {1}'.format(row, ex))


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/trainstops/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_trainstops(filenames, apiurl, apitoken):
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
    command = TrainImportCommand(
        filenames, apiurl, apitoken, True, expected_header=expected_header,
        encoding='latin1')
    command.run()

if __name__ == '__main__':
    import_trainstops()
