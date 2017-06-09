import time
import csv
from collections import defaultdict
import traceback

import requests
import click

from voa_utils import process
from utils import print_outcomes_and_rate


class CSVStreamImportCommand(object):
    def __init__(
            self, file_name, api_url, token,
            skip_header=False, encoding=None, pdb=False):
        self.api_url = api_url
        self.token = token
        self.file_name = file_name
        self.skip_header = skip_header
        self.encoding = encoding
        self.pdb = pdb

    def process_record(self, record):
        # Process Area payloads
        area_payload = []

        for area_record in record['line_items']:
            area_value = round(
                float(area_record.get('area', '0').replace('N/A', '0')), 2)
            price_value = round(float(area_record.get('price', '0')), 2)
            value_value = round(float(area_record.get('value', '0')), 2)

            area = {
                "floor": area_record.get('floor'),
                "description": area_record.get('description'),
                "area": area_value,
                "price": price_value,
                "value": value_value
            }

            area_payload.append(area)

        # Process Additional payloads
        additional_payload = []

        def to_float_or_none(val):
            if not val:
                return None
            return round(float(val), 2)

        for additional_record in record['additional']:
            size_value = to_float_or_none(additional_record.get('size', '0'))
            price_value = to_float_or_none(additional_record.get('price', '0'))
            value_value = to_float_or_none(
                additional_record.get('value', '0').replace('+', ''))

            additional = {
                "other_oa_description": area_record.get('floor'),
                "description": area_record.get('description'),
                "area": size_value,
                "price": price_value,
                "value": value_value
            }

            additional_payload.append(additional)

        # Process Adjustment payloads
        adjustment_payload = []

        for adjustment_record in record['adjustments']:
            percent_value = to_float_or_none(
                adjustment_record.get('percent', '0').replace('%', ''))

            adjustment = {
                "description": adjustment_record.get('description'),
                "description": percent_value,
            }

            adjustment_payload.append(adjustment)

        payload = {
            "areas": area_payload,
            "additionals": additional_payload,
            "adjustments": adjustment_payload,
            "assessment_reference": record[
                'details'].get('assessment_reference'),
            "uarn": record['details'].get('uarn'),
            "ba_code": record['details'].get('ba_code'),
            "firm_name": record['details'].get('firm_name'),
            "number_or_name": record['details'].get('number_or_name'),
            "sub_street_3": record['details'].get('sub_street_3'),
            "sub_street_2": record['details'].get('sub_street_2'),
            "sub_street_1": record['details'].get('sub_street_1'),
            "street": record['details'].get('street'),
            "town": record['details'].get('town'),
            "postal_district": record['details'].get('postal_district'),
            "county": record['details'].get('county'),
            "postcode": record['details'].get('postcode'),
            "scheme_ref": record['details'].get('scheme_ref'),
            "primary_description": record[
                'details'].get('primary_description'),
            "total_area": round(
                float(record['details'].get('total_area', '0')), 2),
            "subtotal": round(
                float(record['details'].get('subtotal', '0')), 2),
            "total_value": round(
                float(record['details'].get('total_value', '0')), 2),
            "adopted_rv": round(
                float(record['details'].get('adopted_rv', '0')), 2),
            "list_year": int(record['details'].get('list_year', '0')),
            "ba_name": record['details'].get('ba_name'),
            "ba_reference_number": record[
                'details'].get('ba_reference_number'),
            "vo_ref": record['details'].get('vo_ref'),
            "from_date": record['details'].get('from_date'),
            "to_date": record['details'].get('to_date'),
            "scat_code_only": record['details'].get('scat_code_only'),
            "unit_of_measurement": record[
                'details'].get('unit_of_measurement'),
            "unadjusted_price": record['details'].get('unadjusted_price')
        }

        if record['adjustment_totals']:
            payload['adjustement_total_before'] = round(float(record[
                'adjustment_totals'].get('total_before')), 2)
            payload['adjustement_total'] = round(float(record[
                'adjustment_totals'].get('total_adjustment')), 2)

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        try:
            response = requests.post(
                self.api_url,
                json=payload,
                headers=headers)
        except Exception as e:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    payload['uarn'], response.text))
            return 'Error POSTing - {}'.format(e)

        if response.status_code == 201:
            #print('{0} imported correctly'.format(payload['uarn']))
            return 'processed'
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    payload['uarn'], response.text))
            return 'Error POSTing - {} {}'.format(response.status_code,
                                                  response.text)

    def run(self):
        if self.file_name:
            with open(
                    self.file_name,
                    newline='', encoding=self.encoding) as csvfile:

                reader = csv.reader(csvfile, delimiter='*', quotechar='"')

                start_time = time.time()
                outcomes = defaultdict(list)
                for count, record in enumerate(process(reader)):
                    outcome = None
                    try:
                        outcome = self.process_record(record)
                    except Exception as ex:
                        if 'BdbQuit' in repr(ex):
                            # this allows you to use pdb to quit, otherwise you
                            # need to kill to exit pdb
                            raise
                        print(
                            'ERROR: could not import {0} '
                            'because of: {1}'.format(
                                record['details'].get('uarn'), ex))
                        outcome = 'Error - {}'.format(ex)
                        if self.pdb:
                            traceback.print_exc()
                            import pdb
                            pdb.set_trace()
                    outcomes[outcome or 'processed'].append(
                        record['details'].get('uarn'))
                    if count % 100 == 0:
                        print_outcomes_and_rate(outcomes, start_time)
                        print()
                print_outcomes_and_rate(outcomes, start_time)


@click.command()
@click.option('--filename', help='Addresses *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/voa/',
    help='API url e.g. http://localhost:8000/api/voa/')
@click.option('--apitoken', help='API authentication token')
@click.option('--encoding', default='utf-8',
              help='Encoding of the csv ("utf-8" default, use utf-8-sig" if it'
              ' has a BOM')
@click.option('--pdb', is_flag=True,
              help='On exception, drop into pdb debugger')
def import_addresses(filename, apiurl, apitoken, encoding, pdb):
    command = CSVStreamImportCommand(filename, apiurl, apitoken,
                                     encoding=encoding, pdb=pdb)
    command.run()


if __name__ == '__main__':
    import_addresses()
