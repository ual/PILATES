import os
import logging

logger = logging.getLogger(__name__)


def copy_plans_from_asim(settings):
    asim_output_data_dir = settings['asim_local_output_folder']
    plans_path = os.path.join(asim_output_data_dir, 'final_plans.csv')
    beam_local_plans = os.path.join(
    	settings['beam_local_input_folder'],
    	settings['region'], 'gemini',
    	settings['beam_plans'])

    logger.info("Copying asim plans %s to beam input plans %s", plans_path, beam_local_plans)
    import gzip
    with open(plans_path, 'rb') as f_in, gzip.open(beam_local_plans, 'wb') as f_out:
        f_out.writelines(f_in)
    return
