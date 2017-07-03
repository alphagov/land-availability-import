# NB this script uses 2.8GB memory to store all the polygons before importing
# them. This is needed to aggregate them.
import sys

import requests
import click

from importers import ShapefileImportCommand
from loopstats import LoopStats


class GreenbeltsImportCommand(ShapefileImportCommand):

    def __init__(self, file_name, api_url, token, trial_run):
        assert api_url.endswith('/api/greenbelts/')
        self.greenbelts = {}
        self.processed_identifiers = set()
        self.trial_run = trial_run
        super(GreenbeltsImportCommand, self).__init__(
            file_name, api_url, token)

    def process_record(self, record):
        # e.g. ['Liverpool, Manchester and West Yorks Greenbelt',
        # 'Bolsover District', 'E07000033', 1099.59479897, 18.7505185034]
        # the field names seemed to be in the old geojson file (whereever
        # that came from), but not in the shapefile? so I worked them out by
        # comparing with:
        # https://alasdair.carto.com/tables/local_authority_greenbelt_boundaries_2013_14/public/map
        greenbelt_name, la_name, la_ons_id, perimeter_km, area_ha = \
            record.record
        # The Shapefile splits up each Greenbelt into Polygon shapes in
        # mostly consecutive records (apart from North East Greenbelt), so we
        # need to aggregate them into MultiPolygons
        # e.g. aggregate these records:
        # ['Stoke Greenbelt', 'Newcastle-under-Lyme District (B)', 'E07000195', 13.3962570581, 3.27651873009]
        # ['Stoke Greenbelt', 'Newcastle-under-Lyme District (B)', 'E07000195', 109.047552065, 8.93446203709]
        # ['Stoke Greenbelt', 'Newcastle-under-Lyme District (B)', 'E07000195', 5.47853192949, 1.2090565881]
        greenbelt_identifier = '{} | {}'.format(greenbelt_name, la_name)

        if greenbelt_identifier not in self.greenbelts:
            self.greenbelts[greenbelt_identifier] = dict(
                greenbelt_name=greenbelt_name,
                la_name=la_name,
                la_ons_id=la_ons_id,
                perimeter_km=0.0,
                area_ha=0.0,
                shape={
                    "type": "MultiPolygon",
                    "coordinates": []
                    }
                )
            result = 'new greenbelt'
        else:
            result = 'added to existing greenbelt'
        greenbelt = self.greenbelts[greenbelt_identifier]
        greenbelt['perimeter_km'] += perimeter_km
        greenbelt['area_ha'] += area_ha
        shape = record.shape.__geo_interface__
        if shape['type'] == 'MultiPolygon':
            greenbelt['shape']['coordinates'].extend(
                shape['coordinates'])
        elif shape['type'] == 'Polygon':
            greenbelt['shape']['coordinates'].append(
                shape['coordinates'])
        else:
            raise Exception(
                'ERROR: not sure how to deal with shape {}'.format(
                    shape['type']))
        return result

    def postprocess(self):
        if self.trial_run:
            print('Skipping import (because --trial-run)')
            sys.exit(0)

        loop_stats = LoopStats(len(self.greenbelts))
        print ('\nImporting {} greenbelts'.format(len(self.greenbelts)))
        for greenbelt_identifier, greenbelt in self.greenbelts.items():
            outcome = self.import_(greenbelt_identifier=greenbelt_identifier,
                                   **greenbelt)
            loop_stats.add(outcome, greenbelt_identifier)
            loop_stats.print_every_x_iterations(10)
        print(loop_stats)

    def import_(self, greenbelt_identifier=None, greenbelt_name=None,
                la_name=None, la_ons_id=None, perimeter_km=None, area_ha=None,
                shape=None):
        print('Importing: {} / {}'.format(greenbelt_name, la_name))

        data = {
            "code": greenbelt_identifier,  # couldn't see it in the shapefile,
                                           # so invent an unique string
            "la_name": la_name,
            "gb_name": greenbelt_name,
            "ons_code": la_ons_id,
            "year": None,  # couldn't see it in the shapefile
            "area": round(float(area_ha), 2),
            "perimeter": round(
                float(perimeter_km), 2),
            "geom": shape,
            "srid": 4326
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=data,
            headers=headers)

        if response.status_code == 404:
            print('API URL returns 404 Not Found: {}'.format(self.api_url))
            sys.exit(1)
        if response.status_code != 201:
            print(
                'ERROR: could not import {} - {} {}'.format(
                    greenbelt_name, response.status_code, response.text))
            return 'ERROR: could not import - {} {}'.format(
                response.status_code, response.text)

        #print('{0} imported correctly'.format(greenbelt_name))
        return 'imported'


@click.command()
@click.option('--filename', help='Greenbelts *.shp file (ie unzipped)')
@click.option('--apiurl',
              default='http://localhost:8000/api/greenbelts/',
              help='Land Availability API url, with ending /api/greenbelts/')
@click.option('--apitoken', help='API authentication token')
@click.option('--trial-run', is_flag=True, help='Does everything up to but '
              'not including writing the data to the API')
def import_greenbelts(filename, apiurl, apitoken, trial_run):
    command = GreenbeltsImportCommand(filename, apiurl, apitoken, trial_run)
    command.run()


if __name__ == '__main__':
    import_greenbelts()
