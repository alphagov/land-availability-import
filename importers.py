import csv
import shapefile
from collections import defaultdict
import time

from utils import print_outcomes_and_rate


class CSVImportCommand(object):
    def __init__(
            self, file_names, api_url, token,
            skip_header=False, encoding=None):
        self.api_url = api_url
        self.token = token
        self.file_names = file_names
        self.skip_header = skip_header
        self.encoding = encoding

    def process_row(self, row):
        pass

    def run(self):
        for file_name in self.file_names:
            print('Processing {0}'.format(file_name))
            with open(
                    file_name,
                    newline='', encoding=self.encoding) as csvfile:

                reader = csv.reader(csvfile, delimiter=',', quotechar='"')

                if self.skip_header:
                    next(reader)

                outcomes = defaultdict(list)
                start_time = time.time()
                for count, row in enumerate(reader):
                    outcome = self.process_row(row)
                    outcomes[outcome or 'processed'].append(row)
                    if count % 100 == 0:
                        print_outcomes_and_rate(
                            outcomes, start_time)
                        print()
        print_outcomes_and_rate(
            outcomes, start_time)


class ShapefileImportCommand(object):
    def __init__(self, file_name, api_url, token):
        self.api_url = api_url
        self.token = token
        self.file_name = file_name

    def process_record(self, record):
        pass

    def run(self):
        print('Processing shapefile {}'.format(self.file_name))
        outcomes = defaultdict(list)
        start_time = time.time()
        shp_reader = shapefile.Reader(self.file_name)
        for count, record in enumerate(shp_reader.iterShapeRecords()):
            if record.shape.shapeType == shapefile.NULL:
                outcomes['no shapefile'].append(record.record[0])
                continue
            outcome = self.process_record(record)
            outcomes[outcome or 'processed'].append(record.record[0])
            if count % 100 == 0:
                print_outcomes_and_rate(outcomes, start_time)
