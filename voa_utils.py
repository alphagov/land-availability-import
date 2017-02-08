import io
import json
import sys


def generate_record(row, titles):
    data = {}
    pos = 0
    for cell in row:
        data[titles[pos]] = cell
        pos += 1
    return data

type01_titles = [
        'assessment_reference', 'uarn', 'ba_code', 'firm_name',
        'number_or_name', 'sub_street_3', 'sub_street_2', 'sub_street_1',
        'street', 'town', 'postal_district', 'county', 'postcode',
        'scheme_ref', 'primary_description', 'total_area', 'subtotal',
        'total_value', 'adopted_rv', 'list_year', 'ba_name',
        'ba_reference_number', 'vo_ref', 'from_date', 'to_date',
        'scat_code_only', 'unit_of_measurement', 'unadjusted_price']

type02_titles = ['line', 'floor', 'description', 'area', 'price', 'value']
type03_titles = ['other_oa_description', 'size', 'price', 'value']
type04_titles = ['pm_value']
type05_titles = ['spaces', 'spaces_value', 'area', 'area_value', 'total']
type06_titles = ['description', 'percent']
type07_titles = ['total_before', 'total_adjustment']


processors = {
    '01': ('details', type01_titles,),
    '02': ('line_items', type02_titles,),
    '03': ('additional', type03_titles),
    '04': ('plant_machinery', type04_titles),
    '05': ('carpark', type05_titles),
    '06': ('adjustments', type06_titles),
    '07': ('adjustment_totals', type07_titles),
}


def new_record():
    return {
        'line_items': [],
        'additional': [],
        'adjustments': [],
        'plant_machinery': None,
        'details': None,
        'carpark': None,
        'adjustment_totals': None
    }


def process(row_feeder):
    current_record = new_record()
    for row in row_feeder:
        id_field = row[0]

        # Get the name of this row and titles to use
        name, title = processors[id_field]

        # If this is a 01 and we have data from
        # processing then we should yield the
        # record
        if id_field == '01' and current_record.get('details'):
            yield current_record
            current_record = new_record()

        data = generate_record(row[1:], title)

        if id_field in ['02', '03', '06']:
            current_record[name].append(data)
        else:
            current_record[name] = data

    if current_record:
        yield current_record
