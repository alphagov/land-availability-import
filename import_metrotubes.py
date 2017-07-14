from importers import CSVImportCommand
import requests
import click


class MetroTubeImportCommand(CSVImportCommand):

    def process_row(self, row):
        try:
            if row[31] in ['TMU', 'MET', 'PLT']:
                data = {
                    "atco_code": row[0],
                    "name": row[4],
                    "naptan_code": row[1],
                    "locality": row[18],
                    "point": {
                        "type": "Point",
                        "coordinates": [float(row[29]), float(row[30])]
                    },
                    "srid": 4326
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
        except UnicodeDecodeError as ex:
            print(
                'ERROR: could not import {0} because of {1}'.format(row, ex))


@click.command()
@click.argument('filenames', nargs=-1, type=click.Path())
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/metrotubes/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_metrotubes(filenames, apiurl, apitoken):
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
    command = MetroTubeImportCommand(
        filenames, apiurl, apitoken, True, encoding='ISO-8859-1',
        expected_header=expected_header)
    command.run()

if __name__ == '__main__':
    import_metrotubes()
