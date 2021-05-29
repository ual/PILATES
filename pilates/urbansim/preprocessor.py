import pandas as pd
import logging
import os
from pilates.utils.geog import map_block_to_taz

logger = logging.getLogger(__name__)

beam_skims_types = {'timePeriod': str,
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


def _load_raw_skims(settings):

    path_to_skims = settings.get('path_to_skims', False)

    if not path_to_skims:
        logger.info(
            "No path to skims specified at runtime. The ActivitySim "
            "container is gonna go looking for them. You were warned.")
        return
    else:
        try:
            # load skims from disk or url
            skims = pd.read_csv(path_to_skims, dtype=beam_skims_types)
        except KeyError:
            raise KeyError(
                "Couldn't find input skims at {0}".format(path_to_skims))

    return skims


def usim_model_data_fname(region_id):
    return 'custom_mpo_{0}_model_data.h5'.format(region_id)


def add_skims_to_model_data(settings, region):
    logger.info("Loading skims from disk")
    df = _load_raw_skims(settings)

    logger.info("Converting skims to UrbanSim data format.")
    df = df.loc[(df['pathType'] == 'SOV') & (df['timePeriod'] == 'AM')]
    df = df[['origin', 'destination', 'TOTIVT_IVT_minutes', 'DIST_meters']]
    df = df.rename(columns={
        'origin': 'from_zone_id',
        'destination': 'to_zone_id',
        'TOTIVT_IVT_minutes': 'SOV_AM_IVT_mins'})
    df['from_zone_id'] = df['from_zone_id'].astype('str')
    df['to_zone_id'] = df['to_zone_id'].astype('str')
    df = df.set_index(['from_zone_id', 'to_zone_id'])
    region_id = settings['region_to_region_id'][region]
    model_data_fname = usim_model_data_fname(region_id)
    model_data_fpath = os.path.join(
        settings['usim_local_data_folder'], model_data_fname)
    store = pd.HDFStore(model_data_fpath)
    store['travel_data'] = df
    del df

    # should only have to be run the first time the raw
    # urbansim data is touched by pilates
    zone_id_col = 'zone_id'  # col name we want at the end
    blocks = store['blocks'].copy()
    if zone_id_col not in blocks.columns:
        logger.info("Mapping block IDs to TAZ")
        ref_zone_id_col = 'objectid'  # col name from remote data source
        block_taz = map_block_to_taz(
            settings, region, zone_id_col=zone_id_col,
            reference_taz_id_col=ref_zone_id_col)
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
