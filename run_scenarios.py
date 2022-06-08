import os 
import shutil
import yaml
import sys
import pandas as pd

from pilates.results.results import get_scenario_resutls, plot_results
from pilates.scripts.modify_capacities import tod_scenario
from pilates.utils.io import read_yaml, save_yaml
from run import formatted_print, _warm_start_activities, initialize_docker_client, forecast_land_use,generate_activity_plans

import logging
logging.basicConfig(level=logging.INFO, stream=sys.stdout,
                    format='%(asctime)s %(name)s - %(levelname)s - %(message)s',
                    datefmt='%m/%d/%Y %I:%M:%S %p')

        
def create_path(path, replace = False):
    """ Create path if path is not defined. If replace is True, removes and create empty path"""
    try:
        os.makedirs(path)
    except FileExistsError:
        if replace:
            shutil.rmtree(path)
            os.makedirs(path)
        else:
            pass
            
def check_warm_start(settings):
    region = settings['region']
    region_id = settings['region_to_region_id'][region]
    formattable_name = settings['usim_formattable_input_file_name']
    store_fpath = os.path.join('pilates','urbansim','data',
                               formattable_name.format(region_id= region_id))
    store = pd.HDFStore(store_fpath)
    
    #Fast way to just return column names 
    persons_cols = list(store.select('/persons', start = 0, stop = 1).columns)
    households_cols = list(store.select('/households', start = 0, stop = 1).columns)
    total_cols = persons_cols + households_cols
    warm_start_exist = pd.Series(['work_zone_id','school_zone_id','cars']).isin(total_cols).all()
    store.close()
    return warm_start_exist

def check_urbansim(settings):
    
    year = settings['end_year']
    formattable_name = settings['usim_formattable_output_file_name']
    store_fpath = os.path.join('pilates','urbansim','data',
                               formattable_name.format(year = year))

    if os.path.isfile(store_fpath):
        store = pd.HDFStore(store_fpath)
        urbansim_exist = len(store.keys()) > 0
        store.close()
        return urbansim_exist
    else:
        return False
    
def run_one_year(settings):
    asim_dir = settings['region_to_asim_subdir']['sfbay']
    logging.info('ActivitySim Subdirectory: {}'.format(asim_dir))
    
    start_year = settings['start_year']
    end_year = settings['end_year']
    travel_model = settings.get('travel_model', False)
    formatted_print(
        'RUNNING PILATES FROM {0} TO {1}'.format(start_year, end_year))
    travel_model_freq = settings.get('travel_model_freq', 1)
    container_manager = settings['container_manager']
    settings['land_use_enabled'] = True
    settings['traffic_assignment_enabled'] = False
    
    client = initialize_docker_client(settings)
    year = start_year
    forecast_year = end_year
    
    warm_start_exist = check_warm_start(settings)
    
    if not warm_start_exist:
        _warm_start_activities(settings, year, client)
    
    urbansim_exist = check_urbansim(settings)
    
    if not urbansim_exist:
        forecast_land_use(settings, year, forecast_year, client, container_manager)
    else: 
        logging.info('Land Use Simulation found. Skip ...')
        
    generate_activity_plans(settings, year, forecast_year, client, warm_start=False)
    
            
if __name__ == '__main__':
    
    settings_fpath = 'settings.yaml'
    settings = read_yaml(settings_fpath)

    policy_fpath = 'policy_settings.yaml'
    policies = read_yaml(policy_fpath)

    scenarios = []
    for p in policies['policies'].keys():
        for s in policies['policies'][p]['scenarios'].keys():
            
            formatted_print(
                'RUNNING POLICY: {0}, SCENARIO: {1}'.format(p,s))
            
            
            scenario_fpath = os.path.join('carb','scenarios', p, s)
            result_summary_fpath = os.path.join('pilates','results', p, s, 'results.yaml')

            if os.path.isfile(result_summary_fpath):
                logging.info('{} already exist. Skip ...'.format(result_summary_fpath))

            else:
                logging.info('Creating {} ...'.format(os.path.join('pilates','results', p, s)))
                create_path(os.path.join('pilates','results', p, s), replace = False)

                if p in ['tod_employment','tod_residential']:
                    
                    if os.path.isfile('pilates/urbansim/data/model_data_2011.h5'):
                        os.remove('pilates/urbansim/data/model_data_2011.h5')
                    
                    settings = read_yaml(settings_fpath)
                    settings['region_to_asim_subdir']['sfbay'] = 'bay_area'
                    save_yaml(settings_fpath, settings)

                    land_use = p
                    factor = policies['policies'][p]['scenarios'][s]
                    tod_scenario(land_use = p, factor = factor)

                else: 
                    #Change settings files: 
                    settings = read_yaml(settings_fpath)
                    settings['region_to_asim_subdir']['sfbay'] = scenario_fpath
                    save_yaml(settings_fpath, settings)

                #Run Simulation
                run_one_year(settings)

                #Save Resutls
                result_summary = get_scenario_resutls(p, s, policies)
                save_yaml(result_summary_fpath, result_summary)
    