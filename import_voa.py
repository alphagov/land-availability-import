import requests
import click
import csv
from voa_utils import process


class CSVStreamImportCommand(object):
    def __init__(
            self, file_name, api_url, token,
            skip_header=False, encoding=None):
        self.api_url = api_url
        self.token = token
        self.file_name = file_name
        self.skip_header = skip_header
        self.encoding = encoding

    def process_record(self, record):
        # Process Area payloads
        area_payload = []

        for area_record in record['line_items']:
            area_value = round(
                float(area_record['area'].replace('N/A', '0')), 2)
            price_value = round(float(area_record['price']), 2)
            value_value = round(float(area_record['value']), 2)

            area = {
                "floor": area_record['floor'],
                "description": area_record['description'],
                "area": area_value,
                "price": price_value,
                "value": value_value
            }

            area_payload.append(area)

        # Process Additional payloads
        additional_payload = []

        for additional_record in record['additional']:
            size_value = round(float(additional_record['size']), 2)
            price_value = round(float(additional_record['price']), 2)
            value_value = round(
                float(additional_record['value'].replace('+', '')), 2)

            additional = {
                "other_oa_description": area_record['floor'],
                "description": area_record['description'],
                "area": size_value,
                "price": price_value,
                "value": value_value
            }

            additional_payload.append(additional)

        # Process Adjustment payloads
        adjustment_payload = []

        for adjustment_record in record['adjustments']:
            percent_value = round(
                float(adjustment_record['percent'].replace('%', '')), 2)

            adjustment = {
                "description": adjustment_record['description'],
                "description": percent_value,
            }

            adjustment_payload.append(adjustment)

        payload = {
            "area": area_payload,
            "additional": additional_payload,
            "adjustment": adjustment_payload,
            "assessment_reference": record['details']['assessment_reference'],
            "uarn": record['details']['uarn'],
            "ba_code": record['details']['ba_code'],
            "firm_name": record['details']['firm_name'],
            "number_or_name": record['details']['number_or_name'],
            "sub_street_3": record['details']['sub_street_3'],
            "sub_street_2": record['details']['sub_street_2'],
            "sub_street_1": record['details']['sub_street_1'],
            "street": record['details']['street'],
            "town": record['details']['town'],
            "postal_district": record['details']['postal_district'],
            "county": record['details']['county'],
            "postcode": record['details']['postcode'],
            "scheme_ref": record['details']['scheme_ref'],
            "primary_description": record['details']['primary_description'],
            "total_area": round(float(record['details']['total_area']), 2),
            "subtotal": round(float(record['details']['subtotal']), 2),
            "total_value": round(float(record['details']['total_value']), 2),
            "adopted_rv": round(float(record['details']['adopted_rv']), 2),
            "list_year": int(record['details']['list_year']),
            "ba_name": record['details']['ba_name'],
            "ba_reference_number": record['details']['ba_reference_number'],
            "vo_ref": record['details']['vo_ref'],
            "from_date": record['details']['from_date'],
            "to_date": record['details']['to_date'],
            "scat_code_only": record['details']['scat_code_only'],
            "unit_of_measurement": record['details']['unit_of_measurement'],
            "unadjusted_price": record['details']['unadjusted_price']
        }

        if record['adjustment_totals']:
            payload['adjustement_total_before'] = round(float(record[
                'adjustment_totals']['total_before']), 2)
            payload['adjustement_total'] = round(float(record[
                'adjustment_totals']['total_adjustment']), 2)

        headers = {'Authorization': 'Token {0}'.format(self.token)}

        response = requests.post(
            self.api_url,
            json=payload,
            headers=headers)

        if response.status_code == 201:
            print('{0} imported correctly'.format(payload['uarn']))
        else:
            print(
                'ERROR: could not import {0} because of {1}'.format(
                    payload['uarn'], response.text))

    def run(self):
        if self.file_name:
            with open(
                    self.file_name,
                    newline='', encoding=self.encoding) as csvfile:

                reader = csv.reader(csvfile, delimiter='*', quotechar='"')

                for record in process(reader):
                    self.process_record(record)


@click.command()
@click.option('--filename', help='Addresses *.csv file')
@click.option(
    '--apiurl',
    default='http://localhost:8000/api/voa/', help='API url')
@click.option('--apitoken', help='API authentication token')
def import_addresses(filename, apiurl, apitoken):
    command = CSVStreamImportCommand(filename, apiurl, apitoken)
    command.run()

if __name__ == '__main__':
    import_addresses()
