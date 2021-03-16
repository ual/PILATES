import boto3
from botocore.exceptions import ClientError
import logging
import pandas as pd
import zipfile
import os
logger = logging.getLogger(__name__)


def exists_on_s3(s3_client, bucket, key):
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except ClientError as e:
        return int(e.response['Error']['Code']) != 404
    return True


def write_outputs_to_s3(data_dir, settings):

    s3_output = settings.get('s3_output', False)
    if s3_output is False:
        return

    logger.info("Writing outputs to s3!")

    updated_tables = ['households', 'persons']

    # run vars
    bucket = settings['bucket_name']
    scenario = settings['scenario']
    year = settings['year']
    if not isinstance(year, str):
        year = str(year)

    # 1. LOAD ASIM OUTPUTS
    output_tables_settings = settings['output_tables']
    h5_output = output_tables_settings['h5_store']
    prefix = output_tables_settings['prefix']
    output_tables = output_tables_settings['tables']

    asim_output_dict = {}
    logger.info("H5 storage setting is {0}!".format(str(h5_output)))
    if h5_output is False:
        for table_name in output_tables:
            file_name = "%s%s.csv" % (prefix, table_name)
            file_path = config.output_file_path(file_name)
            if table_name == 'persons':
                index_col = 'person_id'
            elif table_name == 'households':
                index_col = 'household_id'
            else:
                index_col = None
            asim_output_dict[table_name] = pd.read_csv(
                file_path, index_col=index_col)
    else:
        file_name = '%soutput_tables.h5' % prefix
        file_path = config.output_file_path(file_name)
        store = pd.HDFStore(file_path)
        for table_name in output_tables:
            logger.info(
                "Loading asim output {0} table into memory".format(table_name))
            asim_output_dict[table_name] = store[table_name]
        store.close()

    # 2. LOAD USIM INPUTS
    data_store_path = os.path.join(data_dir, settings['usim_data_store'])

    if not os.path.exists(data_store_path):
        logger.info("Loading input .h5 from s3!")
        remote_s3_path = os.path.join(
            settings['bucket_name'], "input", scenario, year,
            settings['usim_data_store'])
        s3 = boto3.client('s3')
        bucket = remote_s3_path.split('/')[0]
        key = os.path.join(*remote_s3_path.split('/')[1:])
        if not exists_on_s3(s3, bucket, key):
            raise KeyError(
                "No remote model data found using default path. See "
                "simuation.py --help or configs/settings.yaml "
                "for more ideas.")
        with open(data_store_path, 'wb') as f:
            s3.download_fileobj(bucket, key, f)

    logger.info("Loading input .h5 from into memory!")
    input_store = pd.HDFStore(data_store_path)

    required_cols = {}
    for table_name in updated_tables:
        required_cols[table_name] = list(input_store[table_name].columns)

    # 3. PREPARE NEW PERSONS TABLE
    logger.info("Preparing persons table!")
    # new columns to persist: workplace_taz, school_taz
    p_names_dict = {'PNUM': 'member_id'}
    p_cols_to_include = required_cols['persons']
    if 'persons' in asim_output_dict.keys():

        asim_output_dict['persons'].rename(columns=p_names_dict, inplace=True)

        # only preserve original usim columns and two new columns
        for col in ['workplace_taz', 'school_taz']:
            if col not in asim_output_dict['persons'].columns:
                p_cols_to_include.append(col)
        asim_output_dict['persons'] = asim_output_dict['persons'][
            p_cols_to_include]

    # 4. PREPARE NEW HOUSEHOLDS TABLE
    logger.info("Preparing households table!")
    # no new columns to persist, just convert column names
    hh_names_dict = {
        'hhsize': 'persons',
        'num_workers': 'workers',
        'auto_ownership': 'cars',
        'PNUM': 'member_id'}

    if 'households' in asim_output_dict.keys():

        asim_output_dict['households'].rename(
            columns=hh_names_dict, inplace=True)

        # only preserve original usim columns
        asim_output_dict['households'] = asim_output_dict[
            'households'][required_cols['households']]

    # 5. ENSURE MATCHING SCHEMAS FOR UPDATED TABLES
    for table_name in updated_tables:
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
            dtypes = input_store[table_name].dtypes.to_dict()
            for col in required_cols[table_name]:
                if asim_output_dict[table_name][col].dtype != dtypes[col]:
                    asim_output_dict[table_name][col] = asim_output_dict[
                        table_name][col].astype(dtypes[col])

    # specific dtype required conversions
    asim_output_dict['households']['block_id'] = asim_output_dict[
        'households']['block_id'].astype(str)

    # 5. WRITE OUT FOR BEAM
    archive_name = 'asim_outputs.zip'
    outpath = config.output_file_path(archive_name)
    logger.info(
        'Merging results back into UrbanSim format and storing as .zip!')
    with zipfile.ZipFile(outpath, 'w') as csv_zip:

        # copy usim static inputs into archive
        for table_name in input_store.keys():
            logger.info(
                "Zipping {0} input table to output archive!".format(
                    table_name))
            if table_name not in [
                    '/persons', '/households', 'persons', 'households']:
                df = input_store[table_name].reset_index()
                csv_zip.writestr(
                    "{0}.csv".format(table_name), pd.DataFrame(df).to_csv())

        # copy asim outputs into archive
        for table_name in asim_output_dict.keys():
            logger.info(
                "Zipping {0} asim table to output archive!".format(table_name))
            csv_zip.writestr(
                table_name + ".csv", asim_output_dict[table_name].to_csv())
    logger.info("Done creating .zip archive!")

    logger.info("Establishing connection with s3.")
    s3 = boto3.client('s3')
    logger.info("Connected!")

    remote_s3_path = os.path.join(
        bucket, "output", scenario, year, archive_name)
    bucket = remote_s3_path.split('/')[0]
    key = os.path.join(*remote_s3_path.split('/')[1:])
    logger.info("Preparing to write zip archive to {0}".format(remote_s3_path))

    if exists_on_s3(s3, bucket, key):
        logger.info("Archiving old outputs first.")
        last_mod_datetime = s3.head_object(
            Bucket=bucket, Key=key)['LastModified']
        ts = last_mod_datetime.strftime("%Y_%m_%d_%H%M%S")
        new_fname = archive_name.split('.')[0] + \
            '_' + ts + '.' + archive_name.split('.')[-1]
        new_path_elements = remote_s3_path.split("/")[:4] + [
            'archive', new_fname]
        new_fpath = os.path.join(*new_path_elements)
        new_key = os.path.join(*new_fpath.split('/')[1:])
        if exists_on_s3(s3, bucket, new_key):
            # archive already created, just delete og
            s3.delete_object(Bucket=bucket, Key=key)
        else:
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=new_key)
    logger.info('Sending combined data to s3!')
    with open(outpath, 'rb') as archive:
        s3.upload_fileobj(archive, bucket, key)
    logger.info(
        'Zipped archive of results for use in UrbanSim or BEAM now available '
        'at {0}'.format("s3://" + os.path.join(bucket, key)))

    # 6. WRITE OUT FOR USIM
    usim_archive_name = 'model_data.h5'
    outpath_usim = config.output_file_path(usim_archive_name)
    usim_remote_s3_path = os.path.join(
        bucket, 'output', scenario, year, usim_archive_name)
    bucket = usim_remote_s3_path.split('/')[0]
    key = os.path.join(*usim_remote_s3_path.split('/')[1:])
    logger.info(
        'Merging results back into UrbanSim format and storing as .h5!')
    out_store = pd.HDFStore(outpath_usim)

    # copy usim static inputs into archive
    for table_name in input_store.keys():
        logger.info(
            "Copying {0} input table to output store!".format(
                table_name))
        if table_name not in [
                '/persons', '/households', 'persons', 'households']:
            out_store.put(table_name, input_store[table_name], format='t')

    # copy asim outputs into archive
    for table_name in updated_tables:
        logger.info(
            "Copying {0} asim table to output store!".format(
                table_name))
        out_store.put(table_name, asim_output_dict[table_name], format='t')

    out_store.close()
    logger.info("Copying outputs to UrbanSim inputs!")
    if exists_on_s3(s3, bucket, key):
        logger.info("Archiving old outputs first.")
        last_mod_datetime = s3.head_object(
            Bucket=bucket, Key=key)['LastModified']
        ts = last_mod_datetime.strftime("%Y_%m_%d_%H%M%S")
        new_fname = archive_name.split('.')[0] + \
            '_' + ts + '.' + usim_archive_name.split('.')[-1]
        new_path_elements = usim_remote_s3_path.split("/")[:4] + [
            'archive', new_fname]
        new_fpath = os.path.join(*new_path_elements)
        new_key = os.path.join(*new_fpath.split('/')[1:])
        if exists_on_s3(s3, bucket, new_key):
            # archive already created, just delete og
            s3.delete_object(Bucket=bucket, Key=key)
        else:
            s3.copy_object(
                Bucket=bucket,
                CopySource={'Bucket': bucket, 'Key': key},
                Key=new_key)
    with open(outpath_usim, 'rb') as archive:
        s3.upload_fileobj(archive, bucket, key)
    logger.info(
        'New UrbanSim model data now available '
        'at {0}'.format("s3://" + os.path.join(bucket, key)))

    input_store.close()
