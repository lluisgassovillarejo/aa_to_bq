from importer import Importer

"""
----- Import Files from FTP ------
3 files should be dropped to the FTP each day:
- [report suite] + '_' + [date] + '.txt' = Manifest file
- [report suite] + '_' + [date] + '-lookup_data.tar.gz' = Lookup Tables
- [file_num] + '-' + [reportsuite] + '' + [date] + '.tsv.gz' = Data
Lookup Tables and Data files will be downloaded and then deleted from the FTP. Manifest files are kept on the FTP for a log.
"""


def main():
    importer = Importer()
    importer.run()


if __name__ == '__main__':
    main()
