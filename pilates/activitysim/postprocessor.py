import logging
import pandas as pd
import zipfile
import os
logger = logging.getLogger(__name__)


def _load_asim_outputs(settings):
    output_tables_settings = settings['asim_output_tables']
    prefix = output_tables_settings['prefix']
    output_tables = output_tables_settings['tables']
    asim_output_dict = {}
    for table_name in output_tables:
        file_name = "%s%s.csv" % (prefix, table_name)
        file_path = os.path.join(
            settings['asim_local_output_folder'], file_name)
        if table_name == 'persons':
            index_col = 'person_id'
        elif table_name == 'households':
            index_col = 'household_id'
        else:
            index_col = None
        table = pd.read_csv(file_path, index_col=index_col)

        if 'block_id' in table.columns:
            table['block_id'] = table['block_id'].astype(str).str.zfill(15)
        if 'lcm_county_id' in table.columns:
            table['lcm_county_id'] = table['lcm_county_id'].astype(
                str).str.zfill(5)

        asim_output_dict[table_name] = table

    return asim_output_dict


def _get_usim_datastore_fname(settings, io, year=None):

    if io == 'output':
        datastore_name = settings['usim_formattable_output_file_name'].format(
            year=year)
    elif io == 'input':
        region = settings['region']
        region_id = settings['region_to_region_id'][region]
        usim_base_fname = settings['usim_formattable_input_file_name']
        datastore_name = usim_base_fname.format(region_id=region_id)

    return datastore_name


def _prepare_updated_tables(
        settings, forecast_year, asim_output_dict, tables_updated_by_asim,
        prefix=None):
    """
    Combines ActivitySim and UrbanSim outputs for tables updated by
    ActivitySim (e.g. households and persons)
    """

    data_dir = settings['usim_local_data_folder']

    # e.g. model_data_2012.h5
    usim_output_store_name = _get_usim_datastore_fname(
        settings, io='output', year=forecast_year)
    usim_output_store_path = os.path.join(data_dir, usim_output_store_name)
    if not os.path.exists(usim_output_store_path):
        raise ValueError('No output data store found at {0}'.format(
            usim_output_store_path))
    usim_output_store = pd.HDFStore(usim_output_store_path)

    # ensure we preserve all columns originally in the urbansim outputs
    required_cols = {}
    for table_name in tables_updated_by_asim:
        h5_key = table_name
        if prefix:
            h5_key = os.path.join(str(prefix), h5_key)
        required_cols[table_name] = list(usim_output_store[h5_key].columns)

    # This is the inverse process of asim_pre._update_persons_table()
    p_cols_to_include = required_cols['persons']
    p_cols_to_replace = ['work_zone_id', 'school_zone_id']
    p_names_dict = {
        'PNUM': 'member_id',
        'workplace_taz': 'work_zone_id',
        'school_taz': 'school_zone_id',
    }
    if 'persons' in asim_output_dict.keys():
        logger.info("Preparing persons table!")
        for col in p_cols_to_replace:
            # Double check that work_zone_id and school_zone_id are included
            # bc these aren't native columns to UrbanSim but should be there
            # if "warm start" activities were generated
            if col not in required_cols['persons']:
                p_cols_to_include.append(col)
            if col in asim_output_dict['persons'].columns:
                del asim_output_dict['persons'][col]
        asim_output_dict['persons'].rename(columns=p_names_dict, inplace=True)
        asim_output_dict['persons'] = asim_output_dict['persons'][
            p_cols_to_include]

    logger.info("Preparing households table!")
    # This is the inverse process of asim_pre._update_households_table()
    # no new columns to persist, just convert column names
    hh_names_dict = {
        'hhsize': 'persons',
        'num_workers': 'workers',
        'auto_ownership': 'cars',
    }
    hh_cols_to_replace = ['cars']
    hh_cols_to_include = required_cols['households']
    if 'households' in asim_output_dict.keys():
        for col in hh_cols_to_replace:
            if col not in required_cols['households']:
                hh_cols_to_include.append(col)
            if col in asim_output_dict['households'].columns:
                del asim_output_dict['households'][col]
        asim_output_dict['households'].rename(
            columns=hh_names_dict, inplace=True)
        # only preserve original usim columns
        asim_output_dict['households'] = asim_output_dict[
            'households'][required_cols['households']]

    for table_name in tables_updated_by_asim:
        h5_key = table_name
        if prefix:
            h5_key = os.path.join(str(prefix), h5_key)
        logger.info(
            "Validating data schemas for table {0}.".format(table_name))

        # make sure all required columns are present
        if not all([
                col in asim_output_dict[table_name].columns
                for col in required_cols[table_name]]):
            raise KeyError(
                "Not all required columns are in the {0} table!".format(
                    table_name))

        # make sure data types match
        else:
            dtypes = usim_output_store[h5_key].dtypes.to_dict()
            for col in required_cols[table_name]:
                if asim_output_dict[table_name][col].dtype != dtypes[col]:
                    asim_output_dict[table_name][col] = asim_output_dict[
                        table_name][col].astype(dtypes[col])

    usim_output_store.close()

    # specific dtype required conversions
    asim_output_dict['households']['block_id'] = asim_output_dict[
        'households']['block_id'].astype(str)

    return asim_output_dict


def create_beam_input_data(settings, forecast_year, asim_output_dict):

    asim_output_data_dir = settings['asim_local_output_folder']
    archive_name = 'asim_outputs_{0}.zip'.format(forecast_year)
    outpath = os.path.join(asim_output_data_dir, archive_name)
    logger.info(
        'Merging results back into UrbanSim format and storing as .zip!')

    with zipfile.ZipFile(outpath, 'w') as csv_zip:

        # copy asim outputs into archive
        for table_name in asim_output_dict.keys():
            logger.info(
                "Zipping {0} asim table to output archive!".format(table_name))
            csv_zip.writestr(
                table_name + ".csv", asim_output_dict[table_name].to_csv())
    logger.info("Done creating .zip archive!")


def create_usim_input_data(
        settings, input_year, forecast_year, asim_output_dict,
        tables_updated_by_asim):
    """
    Creates UrbanSim input data for the next iteration.

    Populates an .h5 datastore from three sources in order: 1. ActivitySim
    outputs; 2. UrbanSim outputs; 3. UrbanSim inputs. The three sources will
    have tables in common, so care must be taken to use only the most
    recently updated version of each table. In a given iteration, ActivitySim
    runs last, thus UrbanSim outputs are only passed on if they weren't found
    in the ActivitySim outputs. Likewise, UrbanSim *inputs* are only passed
    on to the next iteration if they were not found in the UrbanSim *outputs*.
    """

    # parse settings
    data_dir = settings['usim_local_data_folder']

    # Move UrbanSim input store (e.g. custom_mpo_193482435_model_data.h5)
    # to archive (e.g. input_data_for_2015_outputs.h5) because otherwise
    # it will be overwritten in the next step.
    input_datastore_name = _get_usim_datastore_fname(settings, io='input')
    input_store_path = os.path.join(data_dir, input_datastore_name)
    archive_fname = 'input_data_for_{0}_outputs.h5'.format(forecast_year)
    archive_path = input_store_path.replace(
        input_datastore_name, archive_fname)
    if os.path.exists(input_store_path):
        logger.info(
            "Moving urbansim inputs from the previous iteration to {0}".format(
                archive_fname))
        os.rename(input_store_path, archive_path)
    elif not os.path.exists(archive_fname):
        raise ValueError('No input data found at {0} or {1}.'.format(
            input_store_path, archive_path))

    # load last iter UrbanSim input data
    og_input_store = pd.HDFStore(archive_path)

    # load last iter UrbanSim output data
    usim_output_datastore_name = _get_usim_datastore_fname(
        settings, 'output', forecast_year)
    usim_output_store_path = os.path.join(data_dir, usim_output_datastore_name)
    if not os.path.exists(usim_output_store_path):
        raise ValueError('No output data found at {0}.'.format(
            usim_output_store_path))
    usim_output_store, table_prefix_year = read_datastore(settings, forecast_year)

    logger.info(
        'Merging results back into UrbanSim format and storing as .h5!')

    # instantiate empty .h5 store (e.g. custom_mpo_321487234_model_data.h5)
    new_input_store = pd.HDFStore(input_store_path)
    assert len(new_input_store.keys()) == 0

    # Keep track of which tables have already been added (i.e. updated)
    updated_tables = []

    # 1. copy ASIM OUTPUTS into new input data store
    logger.info(
        "Copying ActivitySim outputs to the new Urbansim input store!")
    for table_name in tables_updated_by_asim:
        new_input_store[table_name] = asim_output_dict[table_name]
        updated_tables.append(table_name)

    # 2. copy USIM OUTPUTS into new input data store if not present already
    logger.info((
        "Passing last set of UrbanSim outputs through to the new "
        "Urbansim input store!"))
    for h5_key in output_store.keys():
        table_name = h5_key.split('/')[-1]
        if table_name not in updated_tables:
            if os.path.join('/', table_prefix_year, table_name) == h5_key:
                new_input_store[table_name] = usim_output_store[h5_key]
                updated_tables.append(table_name)

    # 3. copy USIM INPUTS into new input data store if not present already
    logger.info((
        "Passing static UrbanSim inputs through to the new Urbansim "
        "input store!").format(table_name))
    for h5_key in og_input_store.keys():
        table_name = h5_key.split('/')[-1]
        if table_name not in updated_tables:
            new_input_store[table_name] = og_input_store[h5_key]

    og_input_store.close()
    new_input_store.close()
    usim_output_store.close()

    return


def create_next_iter_inputs(settings, year, forecast_year):

    tables_updated_by_asim = ['households', 'persons']
    asim_output_dict = _load_asim_outputs(settings)
    asim_output_dict = _prepare_updated_tables(
        settings, forecast_year, asim_output_dict, tables_updated_by_asim,
        prefix=forecast_year)

    if settings['traffic_assignment_enabled']:
        create_beam_input_data(settings, forecast_year, asim_output_dict)
    create_usim_input_data(
        settings, year, forecast_year, asim_output_dict,
        tables_updated_by_asim)

    return


def update_usim_inputs_after_warm_start(
        settings, usim_data_dir=None, warm_start_dir=None):
    """
    TODO: Combine this method with create_usim_input_data() above
    """

    # load usim data
    if not usim_data_dir:
        usim_data_dir = settings['usim_local_data_folder']
    datastore_name = _get_usim_datastore_fname(settings, io='input')
    input_store_path = os.path.join(usim_data_dir, datastore_name)
    if not os.path.exists(input_store_path):
        raise ValueError('No input data found at {0}'.format(input_store_path))
    usim_datastore = pd.HDFStore(input_store_path)
    p = usim_datastore['persons']
    hh = usim_datastore['households']

    # load warm start data
    if not warm_start_dir:
        warm_start_dir = settings['asim_local_output_folder']
    warm_start_persons = pd.read_csv(
        os.path.join(warm_start_dir, "warm_start_persons.csv"),
        index_col='person_id',
        dtype={'workplace_taz': str, 'school_taz': str})
    warm_start_households = pd.read_csv(
        os.path.join(warm_start_dir, "warm_start_households.csv"),
        index_col='household_id')

    # replace persons and households with warm start data
    assert p.shape[0] == warm_start_persons.shape[0]
    assert hh.shape[0] == warm_start_households.shape[0]

    p['work_zone_id'] = warm_start_persons['workplace_taz'].reindex(p.index)
    p['school_zone_id'] = warm_start_persons['school_taz'].reindex(p.index)
    hh['cars'] = warm_start_households['auto_ownership'].reindex(
        hh.index)

    usim_datastore['persons'] = p
    usim_datastore['households'] = hh

    usim_datastore.close()

    return
