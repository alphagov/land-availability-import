import csv


class CSVImportCommand(object):
    def __init__(
            self, file_name='', api_url, token,
            skip_header=False, encoding=None):
        self.api_url = api_url
        self.token = token
        self.file_name = file_name
        self.skip_header = skip_header
        self.encoding = encoding

    def process_row(self, row):
        pass

    def run(self):
        if self.file_name:
            with open(
                    self.file_name,
                    newline='', encoding=self.encoding) as csvfile:

                reader = csv.reader(csvfile, delimiter=',', quotechar='"')

                if self.skip_header:
                    next(reader)

                for row in reader:
                    self.process_row(row)
