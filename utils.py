import time

def transform_polygons_to_multipolygon(polygons):
    '''This method expects the Polygons ins GeoJSON format and will return a
    MultiPolygon in GeoJSON format too.
    '''
    if len(polygons) == 0:
        return None

    multipolygon = {
        "type": "MultiPolygon",
        "coordinates": [
            []
        ]
    }

    for polygon in polygons:
        multipolygon['coordinates'][0].append(
            polygon['geom']['coordinates'][0])

    return multipolygon

def print_outcomes_and_rate(outcomes, start_time):
    total_count = sum([len(rows) for rows in outcomes.values()])
    rate_per_hour = total_count / (time.time() - start_time) * 60 * 60
    for outcome, rows in outcomes.items():
        print('{:,} {} e.g. {}'.format(len(rows), outcome, rows[0]))
    print('Count: {:,} Rate: {:,.0f}/hour'
          .format(total_count, round(rate_per_hour, -3)))
