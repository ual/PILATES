import pandas as pd
import logging
import os
import h5py

from pilates.utils.geog import map_block_to_taz

logger = logging.getLogger(__name__)

skim_dtypes = {
    'timePeriod': str,
    'pathType': str,
    'origin': int,
    'destination': int,
    'TIME_minutes': float,
    'TOTIVT_IVT_minutes': float,
    'VTOLL_FAR': float,
    'DIST_meters': float,
    'WACC_minutes': float,
    'WAUX_minutes': float,
    'WEGR_minutes': float,
    'DTIM_minutes': float,
    'DDIST_meters': float,
    'KEYIVT_minutes': float,
    'FERRYIVT_minutes': float,
    'BOARDS': float,
    'DEBUG_TEXT': str
}


def _load_raw_skims(settings, skim_format):

    skims_fname = settings.get('skims_fname', False)

    try:
        if skim_format == 'beam':
            path_to_skims = os.path.join(
                settings['beam_local_output_folder'], skims_fname)
            # load skims from disk or url
            skims = pd.read_csv(path_to_skims, dtype=skim_dtypes)
            skims = skims.loc[(
                skims['pathType'] == 'SOV') & (
                skims['timePeriod'] == 'AM')]
            skims = skims[[
                'origin', 'destination', 'TOTIVT_IVT_minutes',
                'DIST_meters']]
            skims = skims.rename(columns={
                'origin': 'from_zone_id',
                'destination': 'to_zone_id',
                'TOTIVT_IVT_minutes': 'SOV_AM_IVT_mins'})
        elif skim_format == 'polaris':
            path_to_skims = os.path.join(
                settings['polaris_local_data_folder'], skims_fname)
            f = h5py.File(path_to_skims, 'r')
            ivtt_8_9 = pd.DataFrame(list(f['auto_skims']['t4']['ivtt']))
            cost_8_9 = pd.DataFrame(list(f['auto_skims']['t4']['cost']))
            f.close()
            ivtt_8_9 = pd.DataFrame(
                ivtt_8_9.stack(), columns=['auto_ivtt_8_9_am'])
            cost_8_9 = pd.DataFrame(
                cost_8_9.stack(), columns=['auto_cost_8_9_am'])
            skims = ivtt_8_9.join(cost_8_9)
            skims.index.names = ['from_zone_id', 'to_zone_id']
            skims = skims.reset_index()
    except KeyError:
        raise KeyError(
            "Couldn't find input skims named {0}".format(skims_fname))

    logger.info("Converting skims to UrbanSim data format.")
    skims['from_zone_id'] = skims['from_zone_id'].astype('str')
    skims['to_zone_id'] = skims['to_zone_id'].astype('str')
    skims = skims.set_index(['from_zone_id', 'to_zone_id'])

    return skims


def usim_model_data_fname(region_id):
    return 'custom_mpo_{0}_model_data.h5'.format(region_id)


def add_skims_to_model_data(
        settings, region, skim_zone_source_id_col):

    logger.info("Loading skims from disk")
    skim_format = settings['travel_model']
    df = _load_raw_skims(settings, skim_format=skim_format)
    region_id = settings['region_to_region_id'][region]
    model_data_fname = usim_model_data_fname(region_id)
    model_data_fpath = os.path.join(
        settings['usim_local_data_folder'], model_data_fname)
    if not os.path.exists(model_data_fpath):
        raise ValueError('No model data found at {0}'.format(
            model_data_fpath))
    store = pd.HDFStore(model_data_fpath)
    store['travel_data'] = df
    del df

    # should only have to be run the first time the raw
    # urbansim data is touched by pilates
    zone_id_col = 'zone_id'  # col name we want at the end
    blocks = store['blocks'].copy()
    if zone_id_col not in blocks.columns:

        block_to_zone_fpath = \
            "pilates/utils/data/{0}/blocks_to_taz.csv".format(region)
        if not os.path.isfile(block_to_zone_fpath):
            logger.info("Mapping block IDs to skim zones")
            block_taz = map_block_to_taz(
                settings, region, zone_id_col=zone_id_col,
                reference_taz_id_col=skim_zone_source_id_col)
            block_taz.to_csv(block_to_zone_fpath)
        else:
            block_taz = pd.read_csv(
                block_to_zone_fpath, dtype={'GEOID': str})
            block_taz = block_taz.set_index('GEOID')[zone_id_col]

        block_taz.index.name = 'block_id'
        blocks = blocks.join(block_taz)
        blocks[zone_id_col] = blocks[zone_id_col].fillna(0)
        blocks = blocks[blocks[zone_id_col] != 0].copy()

        logger.info("Write out to the data store.")
        households = store['households'].copy()
        persons = store['persons'].copy()
        jobs = store['jobs'].copy()
        units = store['residential_units'].copy()
        assert households['block_id'].isin(blocks.index).all()
        assert persons['household_id'].isin(households.index).all()
        assert jobs['block_id'].isin(blocks.index).all()
        assert units['block_id'].isin(blocks.index).all()
        store['blocks'] = blocks
        store['households'] = households
        store['persons'] = persons
        store['jobs'] = jobs
        store['residential_units'] = units

    store.close()
