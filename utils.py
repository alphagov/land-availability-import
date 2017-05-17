def transform_polygons_to_multipolygon(polygons):
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
