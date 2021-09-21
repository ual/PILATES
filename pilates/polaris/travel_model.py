import os
import sys
import subprocess
import yaml
import pilates.polaris.preprocessor as preprocessor
import pilates.polaris.postprocessor as postprocessor
import pilates.polaris.run_convergence as convergence
import logging
import glob
import fnmatch
from pathlib import Path

logger = logging.getLogger(__name__)

def all_subdirs_of(out_name, b='.'):
  result = []
  # for d in os.listdir(b):
  #   bd = os.path.join(b, d)
  #   if os.path.isdir(bd): result.append(bd)
  search_path = '{0}/{1}'.format(b, out_name + '*')
  result = glob.glob(search_path)
  return result

def get_latest_polaris_output(out_name, data_dir='.'):
	# all_subdirs = [d for d in os.listdir(data_dir) if os.path.isdir(d)]
	all_subdirs = all_subdirs_of(out_name, data_dir)
	latest_subdir = Path(max(all_subdirs, key=os.path.getmtime))
	return latest_subdir
	
def update_scenario_file(base_scenario, forecast_year):
	s = base_scenario.replace('.json', '_' + str(forecast_year) + '.json')
	return s

def modify_scenario(scenario_file, parameter, value):
	f = open(scenario_file,'r')
	filedata = f.read()
	datalist = []
	f.close()

	for d in filedata.split(','): 
		datalist.append(d.strip())

	find_str = '"' + parameter + '"*'
	find_match_list = fnmatch.filter(datalist,find_str)

	if len(find_match_list)==0:
		print('Could not find parameter: ' + find_str + ' in scenario file: ' + scenario_file)
		print(datalist)
		sys.exit()
		
	find_match = find_match_list[len(find_match_list)-1]

	newstr = '"' + parameter + '" : ' + str(value)
	newdata = filedata.replace(find_match,newstr)

	f = open(scenario_file,'w')
	f.write(newdata)
	f.close()

def run_polaris(forecast_year, usim_output):
	# read settings from config file
	with open('pilates/polaris/polaris_settings.yaml') as file:
		polaris_settings = yaml.load(file, Loader=yaml.FullLoader)
	data_dir = polaris_settings.get('data_dir')
	scripts_dir = polaris_settings.get('scripts_dir')
	db_name = polaris_settings.get('db_name')
	out_name = polaris_settings.get('out_name')
	polaris_exe = polaris_settings.get('polaris_exe')
	scenario_init_file = polaris_settings.get('scenario_main_init')
	scenario_main_file = polaris_settings.get('scenario_main')
	num_threads = polaris_settings.get('num_threads')
	num_abm_runs = polaris_settings.get('num_abm_runs')
	block_loc_file_name = polaris_settings.get('block_loc_file_name')
	population_scale_factor = polaris_settings.get('population_scale_factor')
	archive_dir = polaris_settings.get('archive_dir')
	db_supply = "{0}/{1}-Supply.sqlite".format(data_dir, db_name)
	db_demand = "{0}/{1}-Demand.sqlite".format(data_dir, db_name)
	block_loc_file = "{0}/{1}".format(data_dir, block_loc_file_name)
	
	preprocessor.preprocess_usim_for_polaris(forecast_year, usim_output, block_loc_file, db_supply, db_demand, population_scale_factor)
	cwd = os.getcwd()
	os.chdir(data_dir)
	
	# store the original inputs
	supply_db_name = db_name + "-Supply.sqlite"
	demand_db_name = db_name + "-Demand.sqlite"
	result_db_name = db_name + "-Result.sqlite"
	highway_skim_file_name = "highway_skim_file.bin"
	transit_skim_file_name = "transit_skim_file.bin"
	
	for loop in range(0, int(num_abm_runs)):
		scenario_file = ''
		if loop == 0:
			scenario_file = update_scenario_file(scenario_init_file, forecast_year)
			modify_scenario(scenario_file, "time_dependent_routing_weight_factor", 1.0)
			modify_scenario(scenario_file, "percent_to_synthesize", population_scale_factor)
			modify_scenario(scenario_file, "traffic_scale_factor", population_scale_factor)
			modify_scenario(scenario_file, "read_population_from_urbansim", 'true')
			modify_scenario(scenario_file, "read_population_from_database", 'false')
			modify_scenario(scenario_file, "replan_workplaces", 'true')
		else:
			scenario_file = update_scenario_file(scenario_main_file, forecast_year)
			modify_scenario(scenario_file, "time_dependent_routing_weight_factor", 1.0/int(loop))
			modify_scenario(scenario_file, "percent_to_synthesize", population_scale_factor)
			modify_scenario(scenario_file, "traffic_scale_factor", population_scale_factor)
			modify_scenario(scenario_file, "read_population_from_urbansim", 'false')
			modify_scenario(scenario_file, "read_population_from_database", 'true')
			modify_scenario(scenario_file, "replan_workplaces", 'true')
			if loop >= 3:
				modify_scenario(scenario_file, "replan_workplaces", 'false')

		arguments = '{0} {1}'.format(scenario_file, str(num_threads))
		logger.info(f'Executing \'{str(exe_name)} {arguments}\'')

		# run executable
		convergence.run_polaris_local(data_dir, polaris_exe, scenario_file, num_threads)
		
		# get output directory and write files into working dir for next run
		output_dir = get_latest_polaris_output(out_name, data_dir)
		convergence.copyreplacefile(output_dir / demand_db_name, data_dir)
		convergence.copyreplacefile(output_dir / result_db_name, data_dir)
		convergence.copyreplacefile(output_dir / highway_skim_file_name, data_dir)
		convergence.execute_sql_script(data_dir / demand_db_name, scripts_dir / "clean_db_after_abm_for_abm.sql")
		convergence.execute_sql_script_with_attach(output_dir / demand_db_name, data_dir / supply_db_name, scripts_dir / "wtf_baseline_analysis.sql")
		
	os.chdir(cwd)
	# find the latest output
	output_dir = get_latest_polaris_output(out_name, data_dir)
	# db_supply = "{0}/{1}-Supply.sqlite".format(output_dir, db_name)
	# db_demand = "{0}/{1}-Demand.sqlite".format(output_dir, db_name)
	# db_result =  "{0}/{1}-Result.sqlite".format(output_dir, db_name)
	# auto_skim = "{0}/{1}".format(output_dir, polaris_settings.get('auto_skim_file'))
	# transit_skim = "{0}/{1}".format(output_dir, polaris_settings.get('transit_skim_file'))
	# vot_level = polaris_settings.get('vot_level')
	# postprocessor.generate_polaris_skims_for_usim( 'pilates/polaris/data', db_name, db_supply, db_demand, db_result, auto_skim, transit_skim, vot_level)
		
	postprocessor.archive_polaris_output(db_name, forecast_year, output_dir, data_dir)
	postprocessor.archive_and_generate_usim_skims(forecast_year, db_name, output_dir)
