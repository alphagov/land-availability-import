import csv
import shapefile
import os
from datetime import datetime

from loopstats import LoopStats
from notifications_python_client.notifications import NotificationsAPIClient


class ImportCommand(object):
    def notify_import_completed(self, imported_files):
        EMAIL_TO_NOTIFY = os.environ.get('EMAIL_TO_NOTIFY')
        NOTIFY_API_TOKEN = os.environ.get('NOTIFY_API_TOKEN')
        NOTIFY_EMAIL_TEMPLATE = os.environ.get('NOTIFY_EMAIL_TEMPLATE')

        if EMAIL_TO_NOTIFY and NOTIFY_API_TOKEN and NOTIFY_EMAIL_TEMPLATE:
            notifications_client = NotificationsAPIClient(NOTIFY_API_TOKEN)

            personalisation = {
                'import_name': self.__class__.__name__,
                'completion_time': datetime.now().isoformat(),
                'imported_files': imported_files,
                'import_url': self.api_url
            }

            response = notify_client.send_email_notification(
                email_address=EMAIL_TO_NOTIFY,
                template_id=NOTIFY_EMAIL_TEMPLATE,
                personalisation=personalisation,
                reference=None
            )


class CSVImportCommand(ImportCommand):
    def __init__(
            self, file_names, api_url, token,
            skip_header=False, encoding=None, expected_header=None):
        self.api_url = api_url
        self.token = token
        self.file_names = file_names
        self.skip_header = skip_header
        self.encoding = encoding
        self.expected_header = expected_header

    def process_row(self, row):
        pass

    def check_header(self, header, expected_header):
        if header != expected_header:
            return False
        return True

    def run(self):
        for file_name in self.file_names:
            print('Processing {0}'.format(file_name))
            with open(
                    file_name,
                    newline='', encoding=self.encoding) as csvfile:

                loopstats = LoopStats()
                reader = csv.reader(csvfile, delimiter=',', quotechar='"')

                # If we want to skip the header from process_row()
                # it means we have an header. We analyse if the header is
                # in the format we expect, if we provide an expected_header
                if self.skip_header:
                    header = next(reader)

                    if 'expected_header' in dir(self):
                        if not self.check_header(header, self.expected_header):
                            print('ERROR - Headers not matching: \n{0}\n{1}'
                                  .format(header, self.expected_header))
                            break

                for count, row in enumerate(reader):
                    outcome = self.process_row(row)
                    loopstats.add(outcome, row)
                    loopstats.print_every_x_iterations(100)

        # Use GOV.UK Notify to send a notification when import is completed
        self.notify_import_completed(''.join(self.file_names))

        print(loopstats)
        if 'postprocess' in dir(self):
            self.postprocess()
            print(loopstats)


class ShapefileImportCommand(ImportCommand):
    def __init__(self, file_name, api_url, token):
        self.api_url = api_url
        self.token = token
        self.file_name = file_name

    def process_record(self, record):
        pass

    def run(self):
        print('Processing shapefile {}'.format(self.file_name))
        loopstats = LoopStats()
        shp_reader = shapefile.Reader(self.file_name)
        for record in shp_reader.iterShapeRecords():
            if record.shape.shapeType == shapefile.NULL:
                loopstats.add('no shapefile', record.record[0])
                continue
            outcome = self.process_record(record)
            loopstats.add(outcome, record.record[0])
            loopstats.print_every_x_iterations(100)

        # Use GOV.UK Notify to send a notification when import is completed
        self.notify_import_completed(self.file_name)

        print(loopstats)
        if 'postprocess' in dir(self):
            self.postprocess()
            print(loopstats)
