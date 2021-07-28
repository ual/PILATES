import os
import sys
import subprocess
import yaml
import pilates.polaris.preprocessor as preprocessor
import pilates.polaris.postprocessor as postprocessor
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
    db_base = polaris_settings.get('db_base')
    polaris_exe = polaris_settings.get('polaris_exe')
    scenario_file = polaris_settings.get('scenario_file')
    num_threads = polaris_settings.get('num_threads')
    block_loc_file_name = polaris_settings.get('block_loc_file_name')
    archive_dir = polaris_settings.get('archive_dir')
    db_supply = "{0}/{1}-Supply.sqlite".format(data_dir, db_base)
    db_demand = "{0}/{1}-Demand.sqlite".format(data_dir, db_base)
    block_loc_file = "{0}/{1}".format(data_dir, block_loc_file_name)
    preprocessor.preprocess_usim_for_polaris(forecast_year, usim_output, block_loc_file, db_supply, db_demand)
    cwd = os.getcwd()
    os.chdir(data_dir)
    run_polaris_local(data_dir, polaris_exe, scenario_file, num_threads)
    os.chdir(cwd)
    # find the latest output
    output_dir = get_latest_polaris_output(data_dir)
    db_supply = "{0}/{1}-Supply.sqlite".format(output_dir, db_base)
    db_demand = "{0}/{1}-Demand.sqlite".format(output_dir, db_base)
    db_result =  "{0}/{1}-Result.sqlite".format(output_dir, db_base)
    auto_skim = polaris_settings.get('auto_skim_file')
    transit_skim = polaris_settings.get('transit_skim_file')
    vot_level = polaris_settings.get('vot_level')
    postprocessor.postprocess_polaris_for_usim(db_base, db_supply, db_demand, db_result, auto_skim, transit_skim, vot_level)
    postprocessor.archive_polaris_output(forecast_year, output_dir, data_dir, archive_dir)

def run_polaris_local(results_dir, exe_name, scenario_file, num_threads):
    # subprocess.call([exeName, arguments])
    # out_file = open(str(results_dir / 'simulation_out.log'), 'w+')
    # err_file = open(str(results_dir / 'simulation_err.log'), 'w+')
    # proc = subprocess.Popen([str(exe_name), str(scenario_file), num_threads], stdout=out_file, stderr=subprocess.PIPE)
    # for line in proc.stderr:
    #     sys.stdout.write(str(line))
    #     err_file.write(str(line))
    # proc.wait()
    # out_file.close()
    # err_file.close()
    proc = subprocess.Popen([str(exe_name), str(scenario_file), num_threads], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    output, err = proc.communicate()
    if proc.returncode != 0:
        logger.critical("POLARIS did not execute correctly")
        exit(1)
