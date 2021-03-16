import os
import openmatrix as omx
import pandas as pd
import numpy as np
import logging

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


def _load_raw_beam_skims(settings):

    path_to_beam_skims = settings.get('path_to_beam_skims', False)

    if not path_to_beam_skims:
        logger.info("No remote path to BEAM skims specified at runtime.")
        return
    else:
        try:
            # load skims from disk or url
            skims = pd.read_csv(path_to_beam_skims, dtype=beam_skims_types)
        except KeyError:
            raise KeyError(
                "Couldn't find input skims at {0}".format(path_to_beam_skims))

    return skims


def _create_skim_object(data_dir):
    skims_path = os.path.join(data_dir, 'skims.omx')
    skims_exist = os.path.exists(skims_path)

    if skims_exist:
        logger.info("Found existing skims, no need to re-create.")
        return False

    else:
        logger.info("Creating skims.omx from BEAM skims")
        skims = omx.open_file(skims_path, 'w')
        skims.close()
        return True


def _create_skims_by_mode(settings):
    """
    Returns 2 OD pandas dataframe for auto and transit
    """
    logger.info("Splitting BEAM skims by mode.")
    skims_df = _load_raw_beam_skims(settings)

    num_hours = skims_df['timePeriod'].nunique()
    num_modes = skims_df['pathType'].nunique()
    num_od_pairs = len(skims_df) / num_hours / num_modes

    # make sure the matrix is square
    num_taz = np.sqrt(num_od_pairs)
    assert num_taz.is_integer()
    num_taz = int(num_taz)

    # convert beam skims to activitysim units (miles and minutes)
    skims_df['DIST_miles'] = skims_df['DIST_meters'] * (0.621371 / 1000)
    skims_df['DDIST_miles'] = skims_df['DDIST_meters'] * (0.621371 / 1000)

    skims_df = skims_df.sort_values(['origin', 'destination', 'TIME_minutes'])
    logger.info('Splitting out auto skims.')
    auto_df = skims_df.loc[skims_df['pathType'] == 'SOV']
    logger.info('Splitting out transit skims.')
    transit_df = skims_df[
        skims_df['pathType'].isin(settings['transit_paths'])]
    return auto_df, transit_df, num_taz


def _distance_skims(settings, auto_df, data_dir, num_taz):

    # Open skims object
    skims_path = os.path.join(data_dir, 'skims.omx')
    skims = omx.open_file(skims_path, 'a')

    # TO DO: Include walk and bike distances,
    # for now walk and bike are the same as drive.
    distances_auto = auto_df.drop_duplicates(
        ['origin', 'destination'],
        keep='last')[settings['beam_asim_hwy_measure_map']['DIST']]

    # TO DO: Do something better.
    distances_auto = distances_auto.replace(
        0, np.random.normal(39, 20))

    # distances_walk = walk_df.drop_duplicates(
    #     ['origin', 'destination'])[beam_asim_hwy_measure_map['DIST']]

    mx_auto = distances_auto.values.reshape((num_taz, num_taz))
    # mx_walk = distances_walk.values.reshape((num_taz, num_taz))

    # Distance matrices
    skims['DIST'] = mx_auto
    skims['DISTBIKE'] = mx_auto
    skims['DISTWALK'] = mx_auto
    skims.close()


def _transit_access(transit_df, access_paths, num_taz):
    ''' OD pair value for drive access '''
    df = transit_df.loc[transit_df.pathType.isin(access_paths), :]
    df.drop_duplicates(['origin', 'destination'], keep='last', inplace=True)
    assert df.shape[0] == num_taz * num_taz
    return df


def _transit_skims(settings, transit_df, data_dir, num_taz):
    """ Generate transit OMX skims"""
    logger.info("Creating transit skims.")
    # Open skims object
    skims_path = os.path.join(data_dir, 'skims.omx')
    skims = omx.open_file(skims_path, 'a')

    drive_access = ['DRV_COM_WLK', 'DRV_HVY_WLK',
                    'DRV_LOC_WLK', 'DRV_LRF_WLK', 'DRV_EXP_WLK']
    walk_acces = ['WLK_COM_WLK', 'WLK_HVY_WLK', 'WLK_LOC_WLK',
                  'WLK_LRF_WLK', 'WLK_EXP_WLK', 'WLK_TRN_WLK']

    drive_access_values = _transit_access(transit_df, drive_access, num_taz)
    walk_access_values = _transit_access(transit_df, walk_acces, num_taz)

    for path in settings['transit_paths']:

        path_ = path.replace('EXP', "LOC")  # Get the values of LOC for EXP.
        path_ = path_.replace('TRN', "LOC")  # Get the values of LOC for TRN.

        # # When BEAM skims generates all skims
        # mask1 = transit_df['pathType'] == path_
        # df = transit_df[mask1]

        # TO DO: Drive access needs to be different for each transit mode
        # TO DO: Walk access needs to be different for each transit mode
        if path[:4] == 'DRIVE':
            df = drive_access_values
        else:
            df = walk_access_values

        beam_asim_transit_measure_map = settings[
            'beam_asim_transit_measure_map']
        for period in settings['periods']:
            # # When BEAM skims generates all skims
            # mask2 = df_['timePeriod'] == period
            # df_ = df[mask2]
            df_ = df
            for measure in beam_asim_transit_measure_map.keys():
                name = '{0}_{1}__{2}'.format(path, measure, period)
                if beam_asim_transit_measure_map[measure]:
                    vals = df_[beam_asim_transit_measure_map[measure]]
                    mx = vals.values.reshape((num_taz, num_taz), order='C')
                else:
                    mx = np.zeros((num_taz, num_taz))
                skims[name] = mx
    skims.close()


def _auto_skims(settings, auto_df, data_dir, num_taz):
    logger.info("Creating drive skims.")
    # Open skims object
    skims_path = os.path.join(data_dir, 'skims.omx')
    skims = omx.open_file(skims_path, 'a')

    # Create skims
    for period in settings['periods']:
        mask1 = auto_df['timePeriod'] == period
        df = auto_df[mask1]
        beam_asim_hwy_measure_map = settings['beam_asim_hwy_measure_map']
        for path in settings['hwy_paths']:
            for measure in beam_asim_hwy_measure_map.keys():
                name = '{0}_{1}__{2}'.format(path, measure, period)
                if beam_asim_hwy_measure_map[measure]:
                    vals = df[beam_asim_hwy_measure_map[measure]]
                    mx = vals.values.reshape((num_taz, num_taz), order='C')
                else:
                    mx = np.zeros((num_taz, num_taz))
                skims[name] = mx
    skims.close()


def _create_offset(auto_df, data_dir):
    logger.info("Creating skims offset keys")

    # Open skims object
    skims_path = os.path.join(data_dir, 'skims.omx')
    skims = omx.open_file(skims_path, 'a')

    # Generint offset
    taz_equivs = auto_df.origin.sort_values().unique()
    skims.create_mapping('taz', taz_equivs)
    skims.close()


def create_skims_from_beam(data_dir, settings):

    new = _create_skim_object(data_dir)
    if new:
        auto_df, transit_df, num_taz = _create_skims_by_mode(settings)

        # Create skims
        _distance_skims(settings, auto_df, data_dir, num_taz)
        _auto_skims(settings, auto_df, data_dir, num_taz)
        _transit_skims(settings, transit_df, data_dir, num_taz)

        # Create offset
        _create_offset(auto_df, data_dir)
