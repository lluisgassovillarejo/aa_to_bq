import os
import ftplib

from utils import *
from settings import *


class Importer:
    def __init__(self):
        self.downloaded_files = self.get_files_from_ftp()
        self.file_data = self.get_file_data()

    def run(self):
        # Loop through each report suite
        file_metadata = self.file_data
        for report_suite in file_metadata:
            print("Getting files for report suite: {}".format(report_suite))

            # Loop through each date where files exist for this report suite
            for file_dates in file_metadata[report_suite]:
                print("Getting files for {}".format(file_dates))

                # Unzip lookup tables for this date and delete file
                print("Unzipping lookup file: {}".format(file_metadata[report_suite][file_dates]['lookup']))
                unzip_file(file_metadata[report_suite][file_dates]['lookup'])

                # Delete compressed file after extraction
                os.remove(file_metadata[report_suite][file_dates]['lookup'])

                # Loop through each data file that exists for this report suite on this date
                print("{} data files have been created for this date.".format(
                    len(file_metadata[report_suite][file_dates]['data'])))
                for files in file_metadata[report_suite][file_dates]['data']:
                    # Unzip data files
                    unzipped_file_names = []
                    print("Unzipping file: {}".format(files))
                    unzip_file(files)
                    unzipped_file_names.append(files.replace('.gz', ''))

                    # Delete zipped files
                    os.remove(files)

                    print("Unzipped file for: {}".format(files))

                # Ensure that files unzipped properly
                assert (len(unzipped_file_names) > 0)

                # Loop Through Data Files
                data_frames = []
                for file in unzipped_file_names:

                    # Import data as DataFrame
                    df = file_to_frame(file)

                    # Drop preprocessed eVars and Props
                    if (keep_post_only):
                        df = drop_pre_processing_columns(df)

                    # Map values in lookup tables to columns
                    df = map_lookup_file(df, 'browser.tsv', 'browser')
                    df = map_lookup_file(df, 'color_depth.tsv', 'color')
                    df = map_lookup_file(df, 'connection_type.tsv', 'connection_type')
                    df = map_lookup_file(df, 'country.tsv', 'country')
                    df = map_lookup_file(df, 'javascript_version.tsv', 'javascript')
                    df = map_lookup_file(df, 'languages.tsv', 'language')
                    df = map_lookup_file(df, 'operating_systems.tsv', 'os')
                    df = map_lookup_file(df, 'plugins.tsv', 'plugins')
                    df = map_lookup_file(df, 'referrer_type.tsv', 'first_hit_ref_type')
                    df = map_lookup_file(df, 'referrer_type.tsv', 'ref_type')
                    df = map_lookup_file(df, 'referrer_type.tsv', 'visit_ref_type')
                    df = map_lookup_file(df, 'resolution.tsv', 'resolution')
                    df = map_lookup_file(df, 'search_engines.tsv', 'search_engine')
                    df = map_lookup_file(df, 'search_engines.tsv', 'post_search_engine')

                    # Create 'visid_type_map' column to map 'visid_type' to a value for convenience
                    df = create_visitor_id_map(df)

                    # Create Session_ID
                    df = create_session_id(df)

                    # Create User_ID
                    df = create_user_id(df)

                    # Log Data for Validation
                    print_validation(df)

                    # Remove BigQuery invalid characters
                    df.columns = df.columns.str.replace('([^a-zA-Z0-9]|\s)+', '_')

                    # Add dataframe
                    data_frames.append(df)

                    # Delete file
                    os.remove(file)

                final_frame = pd.concat(data_frames)

                # Delete files and lookup tables
                shutil.rmtree('lookup_tables/')
                # Save CSV to 'complete' directory
                export_final_file(final_frame, report_suite, file_dates.replace('-', ''), BQ_PROJECT_ID, BQ_PRIVATE_KEY)

                print("Completed import for {}: {}".format(report_suite, file_dates))
                print("*" * 80)

            print("Files completed for report suite: {}".format(file_metadata[report_suite]))

        print("Files ready to import to BigQuery.")

    def get_file_data(self):
        file_data = {}
        for filename in self.downloaded_files:
            if filename.endswith('-lookup_data.tar.gz'):
                file_type = 'lookup'
                report_suite = filename.split('_')[0]
                file_date = filename.split('_')[1].split('-lookup')[0]
            else:
                file_type = 'data'
                report_suite = filename.split('_')[0]
                report_suite = report_suite.split('-')[1]
                file_date = filename.split('_')[1].split('.')[0]
                file_number = filename.split('-')[0]

            # Create report_suite node if it doesn't exist already
            try:
                file_data[report_suite] = file_data[report_suite]
            except:
                file_data[report_suite] = {}

            # Create file_date node if it doesn't exist already
            try:
                file_data[report_suite][file_date] = file_data[report_suite][file_date]
            except:
                file_data[report_suite][file_date] = {'lookup': '', 'data': []}

            if file_type == 'data':
                file_data[report_suite][file_date]['data'].append(filename)
            else:
                file_data[report_suite][file_date]['lookup'] = filename

        return file_data

    @staticmethod
    def get_files_from_ftp():
        # Connect to FTP
        ftp = ftplib.FTP(FTP_DOMAIN)
        ftp.login(FTP_USERNAME, FTP_PASSWORD)

        # Retrieve List of Files
        filenames = ftp.nlst()

        downloaded_files = []
        for filename in filenames:
            if (filename.endswith('.txt')):
                print("Leaving manifest file on FTP: {}.".format(filename))
            else:
                file = open(filename, 'wb')
                ftp.retrbinary('RETR ' + filename, file.write)
                downloaded_files.append(filename)
                ftp.delete(filename)

        print("{} files successfully downloaded from FTP.".format(len(downloaded_files)))
        ftp.quit()
        return downloaded_files
