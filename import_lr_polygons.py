from importers import ShapefileImportCommand
import requests
import click
from datetime import datetime


class PolygonsImportCommand(ShapefileImportCommand):

    def process_record(self, record):
        polygon_id = record.record[0]
        title_id = record.record[1]
        insert_date = record.record[2]
        update_date = record.record[3]
        status = record.record[4]
        geometry = record.shape.__geo_interface__

        data = {
            "id": polygon_id,
            "title": title_id,
            "insert": insert_date,
            "update": update_date,
            "status": status,
            "geom": geometry,
            "srid": 27700
        }

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=data,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(polygon_id))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    polygon_id, response.text))


@click.command()
@click.option('--filename', help='Polygons *.shp file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/polygons/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_polygons(filename, apiurl, apitoken):
    command = PolygonsImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_polygons()
