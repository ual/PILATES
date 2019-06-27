import pandas as pd
import argparse
import os
import s3fs


year = None
data_mode = 'local'
drop_store = False


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description='Make csvs from H5 store.')

    parser.add_argument('--year', '-y', action='store',
                        help='specify the simulation year')
    parser.add_argument(
        '--datastore-filepath', '-d', action='store',
        dest='datastore_filepath',
        help='full pandas-compatible path to the input data file',
        required=True)
    parser.add_argument(
        '--output-data-dir', '-o', action='store', dest='output_data_dir',
        help='full pandas-compatible path to the output data directory',
        required=True)
    parser.add_argument(
        '--delete-datastore', '-x', action='store_true', dest='drop_store',
        help='delete the datastore after extracting tables')

    options = parser.parse_args()

    if options.year:
        year = options.year
    print('year: {0}'.format(year))

    if options.drop_store:
        drop_store = options.drop_store

    datastore_filepath = options.datastore_filepath
    py2_store = pd.HDFStore(datastore_filepath)

    output_data_dir = options.output_data_dir
    if 's3' in output_data_dir:
        data_mode = 'remote'

    for table in py2_store.keys():

        # if table names have no year prefix or the year
        # prefix matches the specified year
        try:
            if len(table.split('/')) == 2 or year in table:
                if len(table.split('/')) == 3:
                    table_name = table.split('/')[2]
                else:
                    table_name = table.split('/')[1]

                print('table: {0}'.format(table_name))
                df = py2_store[table]

                if data_mode == 'local':
                    if not os.path.exists(output_data_dir):
                        os.makedirs(output_data_dir)

                fname = '{0}.csv'.format(table_name)
                output_filepath = os.path.join(output_data_dir, fname)

                if data_mode == 'local':
                    if os.path.exists(output_filepath):
                        os.remove(output_filepath)
                else:
                    s3 = s3fs.S3FileSystem(anon=False)
                    if s3.exists(output_filepath):
                        s3.rm(output_filepath)

                df.to_csv(output_filepath)

        except TypeError:
            print(
                "If you are seeing this error it is likely that the .h5 "
                "datastore from which you are attempting to extract data has "
                "multiple years' worth of data and you have failed to "
                "specify the year of interest with the [-y] command line "
                "argument.")

    if drop_store:
        os.remove(datastore_filepath)
