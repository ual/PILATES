import os
import pandas as pd


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
    data_dir = settings['data_folder']
    usim_local_data_folder = data_dir / settings['usim_local_data_folder']

    if (year == settings['start_year']) or warm_start:
        usim_datastore = settings['usim_formattable_input_file_name'].format(region_id=region_id)
        table_prefix_yr = ''  # input data store tables have no year prefix

    # Otherwise we read from the land use outputs
    else:
        usim_datastore = settings['usim_formattable_output_file_name'].format(year=year)
        table_prefix_yr = str(year)

    usim_datastore_fpath = usim_local_data_folder / usim_datastore

    if not usim_datastore_fpath.exists():
        raise ValueError(f'No land use data found at {usim_datastore_fpath}!')

    store = pd.HDFStore(usim_datastore_fpath)

    return store, table_prefix_yr
