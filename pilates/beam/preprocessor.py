import os
import logging
import gzip
import shutil
import pandas as pd
import numpy as np

logger = logging.getLogger(__name__)


def copy_plans_from_asim(settings, year, replanning_iteration_number=0):
    asim_output_data_dir = settings['asim_local_output_folder']
    beam_scenario_folder = os.path.join(
        settings['beam_local_input_folder'],
        settings['region'],
        settings['beam_scenario_folder'])

    def copy_with_compression_asim_file_to_beam(asim_file_name, beam_file_name):
        asim_file_path = os.path.join(asim_output_data_dir, asim_file_name)
        beam_file_path = os.path.join(beam_scenario_folder, beam_file_name)
        logger.info("Copying asim file %s to beam input scenario file %s", asim_file_path, beam_file_path)

        if os.path.exists(asim_file_path):
            with open(asim_file_path, 'rb') as f_in, gzip.open(
                    beam_file_path, 'wb') as f_out:
                f_out.writelines(f_in)

    def merge_only_updated_households():
        asim_plans_path = os.path.join(asim_output_data_dir, 'final_plans.csv')
        asim_households_path = os.path.join(asim_output_data_dir, 'final_households.csv')
        asim_persons_path = os.path.join(asim_output_data_dir, 'final_persons.csv')
        beam_plans_path = os.path.join(beam_scenario_folder, 'plans.csv.gz')
        beam_households_path = os.path.join(beam_scenario_folder, 'households.csv.gz')
        beam_persons_path = os.path.join(beam_scenario_folder, 'persons.csv.gz')
        if os.path.exists(beam_plans_path):
            logger.info("Merging asim outputs with existing beam input scenario files")
            original_households = pd.read_csv(beam_households_path)
            updated_households = pd.read_csv(asim_households_path)
            original_persons = pd.read_csv(beam_persons_path)
            updated_persons = pd.read_csv(asim_persons_path)

            per_o = original_persons.person_id.unique()
            per_u = updated_persons.person_id.unique()
            overlap = np.in1d(per_u, per_o).sum()
            logger.info("There were %s persons replanned out of %s originally, and %s of them existed before", len(per_u), len(per_o), overlap)  
            
            hh_o = (original_persons.household_id.unique())
            hh_u = (updated_persons.household_id.unique())
            overlap = np.in1d(hh_u, hh_o).sum()
            logger.info("There were %s households replanned out of %s originally, and %s of them existed before", len(hh_u), len(hh_o), overlap)  

            persons_final = pd.concat([updated_persons, original_persons.loc[~original_persons.person_id.isin(per_u),:]])
            persons_final.to_csv(beam_persons_path, index=False, compression='gzip')
            households_final = pd.concat([updated_households, original_households.loc[~original_households.household_id.isin(hh_u),:]])
            households_final.to_csv(beam_households_path, index=False, compression='gzip')
            
            original_plans = pd.read_csv(beam_plans_path).rename(columns={'tripId':'trip_id'})
            updated_plans = pd.read_csv(asim_plans_path)
            unchanged_plans = original_plans.loc[~original_plans.person_id.isin(per_u), :]
            logger.info("Adding %s new plan elements after and keeping %s from previous iteration", len(updated_plans), len(unchanged_plans))
            plans_final = pd.concat([updated_plans, unchanged_plans])
            persons_with_plans = np.in1d(persons_final.person_id.unique(), plans_final.person_id.unique()).sum()
            logger.info("Of %s persons, %s of them have plans", len(persons_final), persons_with_plans)
            plans_final.to_csv(beam_plans_path, compression='gzip', index=False)
        else:
            logger.info("No plans existed already so copying them directly. THIS IS BAD")
            pd.read_csv(asim_file_path).to_csv(beam_file_path, compression='gzip')

    if replanning_iteration_number < 0:
        copy_with_compression_asim_file_to_beam('final_plans.csv', 'plans.csv.gz')
        copy_with_compression_asim_file_to_beam('final_households.csv', 'households.csv.gz')
        copy_with_compression_asim_file_to_beam('final_persons.csv', 'persons.csv.gz')
    else:
        merge_only_updated_households()

    if settings.get('final_asim_plans_folder', False):
        asim_partial_plans = os.path.join(asim_output_data_dir, 'final_plans.csv')
        beam_local_plans = os.path.join(beam_scenario_folder, 'plans.csv.gz')
        final_plans_name = f"final_plans_{year}_{replanning_iteration_number:02d}.csv.gz"
        partial_plans_name = f"partial_plans_{year}_{replanning_iteration_number:02d}.csv"
        final_plans_location = os.path.join(settings['final_asim_plans_folder'], final_plans_name)
        partial_plans_location = os.path.join(settings['final_asim_plans_folder'], partial_plans_name)
        logger.info("Copying asim plans %s to final asim folder %s", beam_local_plans, final_plans_location)
        shutil.copyfile(beam_local_plans, final_plans_location)
        # shutil.copyfile(asim_partial_plans, partial_plans_location)

    return
