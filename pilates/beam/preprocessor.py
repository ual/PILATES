import os
import logging
import gzip
import shutil

logger = logging.getLogger(__name__)


def copy_plans_from_asim(settings, year, i):
    asim_output_data_dir = settings['asim_local_output_folder']
    plans_path = os.path.join(asim_output_data_dir, 'final_plans.csv')
    beam_local_plans = os.path.join(
        settings['beam_local_input_folder'],
        settings['region'],
        settings['beam_plans'])

    logger.info(
        "Copying asim plans %s to beam input plans %s",
        plans_path, beam_local_plans)

    with open(plans_path, 'rb') as f_in, gzip.open(
            beam_local_plans, 'wb') as f_out:
        f_out.writelines(f_in)

    if settings.get('final_asim_plans_folder', False):
        final_plans_name = f"final_plans_{year}_{i}.csv.gz"
        final_plans_location = os.path.join(settings['final_asim_plans_folder'], final_plans_name)
        logger.info("Copying asim plans %s to final asim folder %s", beam_local_plans, final_plans_location)
        shutil.copyfile(beam_local_plans, final_plans_location)

    return
