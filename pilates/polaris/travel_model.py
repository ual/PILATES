import os
import sys
import subprocess
import yaml
import pilates.polaris.preprocessor as preprocessor
import pilates.polaris.postprocessor as postprocessor
import pilates.polaris.run_convergence as convergence
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def get_latest_polaris_output(data_dir):
    all_subdirs = [d for d in os.listdir(data_dir) if os.path.isdir(d)]
    latest_subdir = Path(max(all_subdirs, key=os.path.getmtime))
    return latest_subdir

def run_polaris(forecast_year, usim_output):
    # read settings from config file
    with open('pilates/polaris/polaris_settings.yaml') as file:
        polaris_settings = yaml.load(file, Loader=yaml.FullLoader)
    data_dir = polaris_settings.get('data_dir')
    db_name = polaris_settings.get('db_name')
    # polaris_exe = polaris_settings.get('polaris_exe')
    # scenario_file = polaris_settings.get('scenario_file')
    # num_threads = polaris_settings.get('num_threads')
    block_loc_file_name = polaris_settings.get('block_loc_file_name')
    population_scale_factor = polaris_settings.get('population_scale_factor')
    archive_dir = polaris_settings.get('archive_dir')
    db_supply = "{0}/{1}-Supply.sqlite".format(data_dir, db_name)
    db_demand = "{0}/{1}-Demand.sqlite".format(data_dir, db_name)
    block_loc_file = "{0}/{1}".format(data_dir, block_loc_file_name)
    preprocessor.preprocess_usim_for_polaris(forecast_year, usim_output, block_loc_file, db_supply, db_demand, population_scale_factor)
    cwd = os.getcwd()
    os.chdir(data_dir)
    # run_polaris_local(data_dir, polaris_exe, scenario_file, num_threads)
    convergence.run_conv(polaris_settings, data_dir, forecast_year)
    os.chdir(cwd)
    # find the latest output
    output_dir = get_latest_polaris_output(data_dir)
    db_supply = "{0}/{1}-Supply.sqlite".format(output_dir, db_name)
    db_demand = "{0}/{1}-Demand.sqlite".format(output_dir, db_name)
    db_result =  "{0}/{1}-Result.sqlite".format(output_dir, db_name)
    auto_skim = polaris_settings.get('auto_skim_file')
    transit_skim = polaris_settings.get('transit_skim_file')
    vot_level = polaris_settings.get('vot_level')
    postprocessor.postprocess_polaris_for_usim(db_name, db_supply, db_demand, db_result, auto_skim, transit_skim, vot_level)
    postprocessor.archive_polaris_output(forecast_year, output_dir, data_dir, archive_dir)
    postprocessor.archive_and_generate_usim_skims(forecast_year, output_dir)
