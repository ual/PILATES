import argparse
import os
import pandas as pd
import yaml


def parse_args_and_settings(settings_file='settings.yaml'):
    # parse command-line args
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='print docker stdout')
    parser.add_argument(
        '-p', '--pull_latest', action='store_true',
        help='pull latest docker images before running')
    parser.add_argument(
        "-h", "--household_sample_size", action="store",
        help="household sample size")
    parser.add_argument(
        "-s", "--static_skims", action="store_true",
        help="bypass traffic assignment, use same skims for every run.")
    parser.add_argument(
        "-w", "--warm_start_skims", action="store_true",
        help="generate full activity plans for the base year only.")
    parser.add_argument(
        '-f', '--figures', action='store_true',
        help='outputs validation figures')
    parser.add_argument(
        '-d', '--disable_model', action='store',
        help=(
            '"l" for land use, "a" for activity demand, '
            '"t" for traffic assignment. Can specify multiple (e.g. "at")'))
    parser.add_argument(
        '-c', '--config', action='store',
        help='config file name')
    args = parser.parse_args()

    if args.config:
        settings_file = args.config

    # read settings from config file
    with open(settings_file) as file:
        settings = yaml.load(file, Loader=yaml.FullLoader)

    # command-line only settings:
    settings.update({
        'static_skims': args.static_skims,
        'warm_start_skims': args.warm_start_skims,
        'asim_validation': args.figures})

    # override .yaml settings with command-line values if command-line
    # values are not False/None
    if args.verbose:
        settings.update({'docker_stdout': args.verbose})
    if args.pull_latest:
        settings.update({'pull_latest': args.pull_latest})
    if args.household_sample_size:
        settings.update({
            'household_sample_size': args.household_sample_size})
    disabled_models = '' if args.disable_model is None else args.disable_model

    # turn models on or off
    land_use_enabled = ((
                            settings.get('land_use_model', False)) and (
                            not settings.get('warm_start_skims')) and (
                                "l" not in disabled_models))

    vehicle_ownership_model_enabled = settings.get('vehicle_ownership_model', False)  ## Atlas
    activity_demand_enabled = ((
                                   settings.get('activity_demand_model', False)) and (
                                       "a" not in disabled_models))
    traffic_assignment_enabled = ((
                                      settings.get('travel_model', False)) and (
                                      not settings['static_skims']) and (
                                          "t" not in disabled_models))
    replanning_enabled = settings.get('replan_iters', 0) > 0

    if activity_demand_enabled:
        if settings['activity_demand_model'] == 'polaris':
            replanning_enabled = False

    settings.update({
        'land_use_enabled': land_use_enabled,
        'vehicle_ownership_model_enabled': vehicle_ownership_model_enabled,  ## Atlas
        'activity_demand_enabled': activity_demand_enabled,
        'traffic_assignment_enabled': traffic_assignment_enabled,
        'replanning_enabled': replanning_enabled})

    # raise errors/warnings for conflicting settings
    if (settings['household_sample_size'] > 0) and land_use_enabled:
        raise ValueError(
            'Land use models must be disabled (explicitly or via "warm '
            'start" mode to use a non-zero household sample size. The '
            'household sample size you specified is {0}'.format(
                settings['household_sample_size']))
    if (settings['atlas_beamac'] > 0) and ((settings['region'] != 'sfbay') or (settings['skims_zone_type'] != 'taz')):
        raise ValueError(
            'atlas_beamac must be 0 (read accessibility from RData) '
            'unless region = sfbay and skims_zone_type = taz. When'
            'atlas_beamac = 1, accessibility is calculated internally. ')

    return settings


def read_datastore(settings, year=None, warm_start=False):
    """
    Access to the land use .H5 data store
    """
    # If `year` is the start year of the whole simulation, or `warm_start` is
    # True, then land use forecasting has been skipped. This is useful for
    # generating "warm start" skims for the base year. In this case, the
    # ActivitySim inputs must be created from the base year land use *inputs*
    # since no land use outputs have been created yet.
    #
    # Otherwise, `year` indicates the forecast year, i.e. the simulation year
    # land use *outputs* to load.

    region = settings['region']
    region_id = settings['region_to_region_id'][region]
    usim_local_data_folder = settings['usim_local_data_folder']

    if (year == settings['start_year']) or (warm_start):
        table_prefix_yr = ''  # input data store tables have no year prefix
        usim_datastore = settings['usim_formattable_input_file_name'].format(
            region_id=region_id)

    # Otherwise we read from the land use outputs
    else:
        usim_datastore = settings['usim_formattable_output_file_name'].format(
            year=year)
        table_prefix_yr = str(year)

    usim_datastore_fpath = os.path.join(usim_local_data_folder, usim_datastore)

    if not os.path.exists(usim_datastore_fpath):
        raise ValueError('No land use data found at {0}!'.format(
            usim_datastore_fpath))

    store = pd.HDFStore(usim_datastore_fpath)

    return store, table_prefix_yr
