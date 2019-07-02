import pandas as pd
import numpy as np
import argparse
import os
import sys
import s3fs
import zipfile

"""
This script takes the individual UrbanSim input .csv files and
compiles them into an (python 2) .h5 data store object, stored
locally, and used for either estimation, simulation or both in
bayarea_urbansim UrbanSim implementation. The last simulation
step in bayarea_urbansim then converts the updated .h5 back to
individual csv's for use in ActivitySynth and elsewhere.

MUST USE PYTHON 2 to run this script.
"""

baseyear = False
beam_bucket = 'urbansim-beam'
skims_data_dir = 's3://' + beam_bucket
skims_fname = '30.skims-smart-23April2019-baseline.csv.gz'
skims_filepath = os.path.join(skims_data_dir, skims_fname)
csv_fnames = {
    'parcels': 'parcels.csv',
    'buildings': 'buildings.csv',
    'jobs': 'jobs.csv',
    'establishments': 'establishments.csv',
    'households': 'households.csv',
    'persons': 'persons.csv',
    'rentals': 'rentals.csv',
    'units': 'units.csv',
    'mtc_skims': 'mtc_skims.csv',
    # 'beam_skims_raw': ,
    'zones': 'zones.csv',
    # the following nodes and edges .csv's aren't used by bayarea_urbansim
    # they're just being loaded here so they can be passed through to the
    # output data directory for use in activitysynth
    'drive_nodes': 'drive_nodes.csv',
    'drive_edges': 'drive_edges.csv',
    'walk_nodes': 'walk_nodes.csv',
    'walk_edges': 'walk_edges.csv',
}
data_store_fname = 'baus_model_data.h5'
get_mtc_data = False


if __name__ == "__main__":

    assert sys.version_info < (3, 0)

    parser = argparse.ArgumentParser(description='Make H5 store from csvs.')

    parser.add_argument(
        '-b', '--baseyear', action='store_true',
        help='specify the simulation year')
    parser.add_argument(
        '-i', '--input-data-dir', action='store', dest='input_data_dir',
        help='full (pandas-compatible) path to input data directory',
        required=True)
    parser.add_argument(
        '-s', '--skims-filepath', action='store', dest='skims_filepath',
        help='path to beam skims file')
    parser.add_argument(
        '-o', '--output-data-dir', action='store', dest='output_data_dir',
        help='full path to the LOCAL output data directory',
        required=True)
    parser.add_argument(
        '-f', '--output-fname', action='store', dest='output_fname',
        help='filename of the .h5 datastore')
    parser.add_argument(
        '-m', '--get-mtc-data', action='store_true', dest='get_mtc_data')

    options = parser.parse_args()

    if options.baseyear:
        baseyear = options.baseyear

    if options.output_fname:
        data_store_fname = options.output_fname

    if options.skims_filepath:
        skims_filepath = options.skims_filepath

    if options.get_mtc_data:
        get_mtc_data = options.get_mtc_data

    input_data_dir = options.input_data_dir
    output_data_dir = options.output_data_dir

    try:
        parcels = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['parcels']),
            index_col='parcel_id', dtype={
                'parcel_id': int, 'block_id': str, 'apn': str})
    except ValueError:
        parcels = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['parcels']),
            index_col='primary_id', dtype={
                'primary_id': int, 'block_id': str, 'apn': str})

    buildings = pd.read_csv(
        os.path.join(input_data_dir, csv_fnames['buildings']),
        index_col='building_id', dtype={'building_id': int, 'parcel_id': int})
    buildings['res_sqft_per_unit'] = buildings[
        'residential_sqft'] / buildings['residential_units']
    buildings['res_sqft_per_unit'][
        buildings['res_sqft_per_unit'] == np.inf] = 0

    try:
        rentals = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['rentals']),
            index_col='pid', dtype={
                'pid': int, 'date': str, 'region': str,
                'neighborhood': str, 'rent': float, 'sqft': float,
                'rent_sqft': float, 'longitude': float,
                'latitude': float, 'county': str, 'fips_block': str,
                'state': str, 'bathrooms': str})
    except ValueError:
        rentals = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['rentals']),
            index_col=0, dtype={
                'date': str, 'region': str,
                'neighborhood': str, 'rent': float, 'sqft': float,
                'rent_sqft': float, 'longitude': float,
                'latitude': float, 'county': str, 'fips_block': str,
                'state': str, 'bathrooms': str})

    units = pd.read_csv(
        os.path.join(input_data_dir, csv_fnames['units']),
        index_col='unit_id', dtype={'unit_id': int, 'building_id': int})

    try:
        households = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['households']),
            index_col='household_id', dtype={
                'household_id': int, 'block_group_id': str, 'state': str,
                'county': str, 'tract': str, 'block_group': str,
                'building_id': int, 'unit_id': int, 'persons': float})
    except ValueError:
        households = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['households']),
            index_col=0, dtype={
                'household_id': int, 'block_group_id': str, 'state': str,
                'county': str, 'tract': str, 'block_group': str,
                'building_id': int, 'unit_id': int, 'persons': float})
        households.index.name = 'household_id'

    try:
        persons = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['persons']),
            index_col='person_id', dtype={
                'person_id': int, 'household_id': int})
    except ValueError:
        persons = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['persons']),
            index_col=0, dtype={'person_id': int, 'household_id': int})
        persons.index.name = 'person_id'

    try:
        jobs = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['jobs']),
            index_col='job_id', dtype={'job_id': int, 'building_id': int})
    except ValueError:
        jobs = pd.read_csv(
            os.path.join(input_data_dir, csv_fnames['jobs']),
            index_col=0, dtype={'job_id': int, 'building_id': int})
        jobs.index.name = 'job_id'

    establishments = pd.read_csv(
        os.path.join(input_data_dir, csv_fnames['establishments']),
        index_col='establishment_id', dtype={
            'establishment_id': int, 'building_id': int,
            'primary_id': int})

    zones = pd.read_csv(
        os.path.join(input_data_dir, csv_fnames['zones']), index_col='zone_id')

    mtc_skims = pd.read_csv(
        os.path.join(input_data_dir, csv_fnames['mtc_skims']), index_col=0)

    # note the beam skims are sourced from a separate data directory,
    # by default in a remote s3 bucket
    beam_skims_raw = pd.read_csv(skims_filepath)
    beam_skims_raw.rename(columns={
        'generalizedCost': 'gen_cost', 'origTaz': 'from_zone_id',
        'destTaz': 'to_zone_id'}, inplace=True)

    # this data store is just a temp file that only needs to exist
    # while the simulation is running. data is stored as csv's
    # before and afterwards. therefore a temporary, relative filepath
    # is specified here.

    output_filepath = os.path.join(output_data_dir, data_store_fname)
    if os.path.exists(output_filepath):
        os.remove(output_filepath)
        print('Deleting existing data store to create the new one...')
    store = pd.HDFStore(output_filepath)

    store.put('parcels', parcels, format='t')
    store.put('units', units, format='t')
    store.put('rentals', rentals, format='t')

    # data pre-processing hasn't yet taken place if
    # starting with base-year input data
    if baseyear:

        store.put('households', households, format='t')
        store.put('jobs', jobs, format='t')
        store.put('buildings', buildings, format='t')

    # if starting from non-base-year (i.e. intra-simulation) data
    # then the pre-processing data steps should have already
    # occurred and we simply rename the main data tables so that
    # bayarea_urbansim doesn't try to re-pre-process them
    else:

        store.put('households_preproc', households, format='t')
        store.put('jobs_preproc', jobs, format='t')
        store.put('buildings_preproc', buildings, format='t')

    store.put('persons', persons, format='t')
    store.put('establishments', establishments, format='t')
    store.put('mtc_skims', mtc_skims, format='t')
    store.put('zones', zones, format='t')
    store.put('beam_skims_raw', beam_skims_raw, format='t')

    # the drive nodes/edges are only included here to get passed thru
    # to activitysynth. the walk nodes/edges are actually used by
    # BAUS to construct neighborhood-scale accessibility variables
    drive_nodes = pd.read_csv(os.path.join(
        input_data_dir, csv_fnames['drive_nodes'])).set_index('osmid')
    drive_edges = pd.read_csv(os.path.join(
        input_data_dir, csv_fnames['drive_edges'])).set_index('uniqueid')
    walk_nodes = pd.read_csv(os.path.join(
        input_data_dir, csv_fnames['walk_nodes'])).set_index('osmid')
    walk_edges = pd.read_csv(os.path.join(
        input_data_dir, csv_fnames['walk_edges'])).set_index('uniqueid')

    store.put('drive_nodes', drive_nodes, format='t')
    store.put('drive_edges', drive_edges, format='t')
    store.put('walk_nodes', walk_nodes)
    store.put('walk_edges', walk_edges)

    store.keys()

    store.close()
    print('UrbanSim model data now available at {0}'.format(
        os.path.abspath(output_filepath)))

    if get_mtc_data:
        print('Getting MTC data')
        s3 = s3fs.S3FileSystem(anon=False)
        with s3.open('urbansim-inputs/MTCDATA.zip', 'rb') as z:
            with zipfile.ZipFile(z) as zip_file:
                for zip_info in zip_file.infolist():
                    if zip_info.filename[-1] == '/':
                        continue
                    zip_info.filename = zip_info.filename.split('/')[-1]
                    zip_file.extract(zip_info, output_data_dir)
