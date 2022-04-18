import os
import sys
import subprocess
import yaml
import shutil
import pilates.polaris.preprocessor as preprocessor
import pilates.polaris.postprocessor as postprocessor
import logging
import glob
import fnmatch
import polarisruntime as PR
from pathlib import Path
from threading import Thread

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

def run_polaris(forecast_year, usim_settings, warm_start=False):

	logger.info('**** RUNNING POLARIS FOR FORECAST YEAR {0}, WARM START MODE = {1}'.format(forecast_year, warm_start))

	# read settings from config file
	with open('pilates/polaris/polaris_settings.yaml') as file:
		polaris_settings = yaml.load(file, Loader=yaml.FullLoader)
	data_dir = Path(polaris_settings.get('data_dir'))
	backup_dir = Path(polaris_settings.get('backup_dir'))
	scripts_dir = Path(polaris_settings.get('scripts_dir'))
	db_name = polaris_settings.get('db_name')
	out_name = polaris_settings.get('out_name')
	polaris_exe = polaris_settings.get('polaris_exe')
	scenario_init_file = polaris_settings.get('scenario_main_init', None)
	scenario_main_file = polaris_settings.get('scenario_main')
	vehicle_file_base = polaris_settings.get('vehicle_file_basename', None)
	vehicle_file_fleet_base = polaris_settings.get('fleet_vehicle_file_basename', None)
	num_threads = polaris_settings.get('num_threads')
	num_abm_runs = polaris_settings.get('num_abm_runs')
	block_loc_file_name = polaris_settings.get('block_loc_file_name')
	population_scale_factor = polaris_settings.get('population_scale_factor')
	archive_dir = polaris_settings.get('archive_dir')
	db_supply = "{0}/{1}-Supply.sqlite".format(str(data_dir), db_name)
	db_demand = "{0}/{1}-Demand.sqlite".format(str(data_dir), db_name)
	block_loc_file = "{0}/{1}".format(str(data_dir), block_loc_file_name)
	vot_level = polaris_settings.get('vot_level')
	
	usim_output_dir = os.path.abspath(usim_settings['usim_local_data_folder'])
	
	# store the original inputs
	supply_db_name = db_name + "-Supply.sqlite"
	demand_db_name = db_name + "-Demand.sqlite"
	result_db_name = db_name + "-Result.sqlite"
	highway_skim_file_name = "highway_skim_file.bin"
	transit_skim_file_name = "transit_skim_file.bin"
	
	
	cwd = os.getcwd()
	os.chdir(data_dir)
	
	#check if warm start and initialize changes
	if warm_start:
		num_abm_runs = 1
		
		# start with fresh demand database from backup (only for warm start
		PR.copyreplacefile(backup_dir / demand_db_name, data_dir)
		
		# load the urbansim population for the init run	
		preprocessor.preprocess_usim_for_polaris(forecast_year, usim_output_dir, block_loc_file, db_supply, db_demand, 1.0, usim_settings)
		
		# update the vehicles table with new costs
		if forecast_year:
			veh_script = "vehicle_operating_cost_" + str(forecast_year) + ".sql"
			PR.execute_sql_script(data_dir / demand_db_name, data_dir / veh_script)

	fail_count = 0
	loop = 0
			
	while loop < int(num_abm_runs):
		scenario_file = ''
		
		# set vehicle distribution file name based on forecast year
		veh_file_name = '"' + vehicle_file_base + '_{0}.txt"'.format(forecast_year)
		fleet_veh_file_name = '"' + vehicle_file_fleet_base + '_{0}.txt"'.format(forecast_year)
		if not forecast_year:
			veh_file_name = '"' + vehicle_file_base + '.txt"'.format(forecast_year)
			fleet_veh_file_name = '"' + vehicle_file_fleet_base + 'txt"'.format(forecast_year)	
			
		if loop == 0:
			if forecast_year:
				scenario_file = PR.update_scenario_file(scenario_init_file, forecast_year)
			else:
				scenario_file = scenario_init_file			
				
			PR.modify_scenario(scenario_file, "time_dependent_routing_weight_factor", 1.0)
			PR.modify_scenario(scenario_file, "read_population_from_database", 'true')
						
			# set warm_start specific settings (that are also modified by loop...)
			if warm_start:
				PR.modify_scenario(scenario_file, "percent_to_synthesize", 1.0)
				PR.modify_scenario(scenario_file, "read_population_from_urbansim", 'true')
				PR.modify_scenario(scenario_file, "warm_start_mode", 'true')
				PR.modify_scenario(scenario_file, "time_dependent_routing", 'false')
				PR.modify_scenario(scenario_file, "multimodal_routing", 'false')
				PR.modify_scenario(scenario_file, "use_tnc_system", 'false')
				PR.modify_scenario(scenario_file, "output_moe_for_assignment_interval", 'false')
				PR.modify_scenario(scenario_file, "output_link_moe_for_simulation_interval" , 'false')
				PR.modify_scenario(scenario_file, "output_link_moe_for_assignment_interval" , 'false')
				PR.modify_scenario(scenario_file, "output_turn_movement_moe_for_assignment_interval" , 'false')
				PR.modify_scenario(scenario_file, "output_turn_movement_moe_for_simulation_interval" , 'false')
				PR.modify_scenario(scenario_file, "output_network_moe_for_simulation_interval" , 'false')
				PR.modify_scenario(scenario_file, "write_skim_tables" , 'false')
				PR.modify_scenario(scenario_file, "write_vehicle_trajectory" , 'false')
				PR.modify_scenario(scenario_file, "write_transit_trajectory" , 'false')
				PR.modify_scenario(scenario_file, "demand_reduction_factor", 1.0)
				PR.modify_scenario(scenario_file, "traffic_scale_factor", 1.0)
			else:
				PR.modify_scenario(scenario_file, "percent_to_synthesize", population_scale_factor)
				PR.modify_scenario(scenario_file, "read_population_from_urbansim", 'false')
				PR.modify_scenario(scenario_file, "warm_start_mode", 'false')
				PR.modify_scenario(scenario_file, "time_dependent_routing", 'false')
				PR.modify_scenario(scenario_file, "multimodal_routing", 'true')
				PR.modify_scenario(scenario_file, "use_tnc_system", 'true')
				PR.modify_scenario(scenario_file, "output_link_moe_for_assignment_interval" , 'true')
				PR.modify_scenario(scenario_file, "output_turn_movement_moe_for_assignment_interval" , 'true')
				PR.modify_scenario(scenario_file, "write_skim_tables" , 'true')
				PR.modify_scenario(scenario_file, "write_vehicle_trajectory" , 'true')			
				PR.modify_scenario(scenario_file, "demand_reduction_factor", population_scale_factor)
				PR.modify_scenario(scenario_file, "traffic_scale_factor", population_scale_factor)
				
			if warm_start and not forecast_year:
				PR.modify_scenario(scenario_file, "replan_workplaces", 'true')
			else:
				PR.modify_scenario(scenario_file, "replan_workplaces", 'false')
		else:
			if forecast_year:
				scenario_file = PR.update_scenario_file(scenario_main_file, forecast_year)
			PR.modify_scenario(scenario_file, "time_dependent_routing_weight_factor", 1.0/int(loop))
			PR.modify_scenario(scenario_file, "percent_to_synthesize", 1.0)
			PR.modify_scenario(scenario_file, "demand_reduction_factor", 1.0)
			PR.modify_scenario(scenario_file, "traffic_scale_factor", population_scale_factor)
			PR.modify_scenario(scenario_file, "read_population_from_urbansim", 'false')
			PR.modify_scenario(scenario_file, "read_population_from_database", 'true')
			PR.modify_scenario(scenario_file, "replan_workplaces", 'false')

		PR.modify_scenario(scenario_file, "vehicle_distribution_file_name", veh_file_name)
		PR.modify_scenario(scenario_file, "fleet_vehicle_distribution_file_name", fleet_veh_file_name)
			
		arguments = '{0} {1}'.format(scenario_file, str(num_threads))
		logger.info(f'Executing \'{str(polaris_exe)} {arguments}\'')

		# run executable
		success = PR.run_polaris_instance(data_dir, polaris_exe, scenario_file, num_threads, None)
		# get output directory and write files into working dir for next run
		output_dir = PR.get_latest_polaris_output(out_name, data_dir)
			
		if success:
			PR.copyreplacefile(output_dir / demand_db_name, data_dir)
			PR.execute_sql_script(data_dir / demand_db_name, scripts_dir / "clean_db_after_abm_for_abm.sql")
			# skip all of the file storage and analysis for warm start runs - only need the demand file
			if not warm_start:
				fail_count = 0		
				# copy the network outputs back to main data directory
				PR.copyreplacefile(output_dir / result_db_name, data_dir)
				PR.copyreplacefile(output_dir / highway_skim_file_name, data_dir)
				PR.copyreplacefile(data_dir / supply_db_name, output_dir)				
			loop += 1
		else:
			fail_count += 1		
			if fail_count >= 3:
				logger.info("POLARIS crashed three times in a row")
				sys.exit()
			else:
				shutil.rmtree(output_dir)
				logger.info(f"Deleting failed results directory for attempt: {loop}")
	
		
	os.chdir(cwd)
	# find the latest output
	output_dir = PR.get_latest_polaris_output(out_name, data_dir)
	# db_supply = "{0}/{1}-Supply.sqlite".format(output_dir, db_name)
	# db_demand = "{0}/{1}-Demand.sqlite".format(output_dir, db_name)
	# db_result =  "{0}/{1}-Result.sqlite".format(output_dir, db_name)
	# auto_skim = "{0}/{1}".format(output_dir, polaris_settings.get('auto_skim_file'))
	# transit_skim = "{0}/{1}".format(output_dir, polaris_settings.get('transit_skim_file'))
	
	# postprocessor.generate_polaris_skims_for_usim( 'pilates/polaris/data', db_name, db_supply, db_demand, db_result, auto_skim, transit_skim, vot_level)
		
	if warm_start:
		postprocessor.update_usim_after_polaris(forecast_year, usim_output_dir, db_demand, usim_settings)
		# store the updated full population demand database for the next warm start round
		PR.copyreplacefile(data_dir / demand_db_name, backup_dir )
	else: 
		archive_dir = postprocessor.archive_polaris_output(db_name, forecast_year, output_dir, data_dir)
		postprocessor.archive_and_generate_usim_skims(forecast_year, db_name, output_dir, vot_level)
		# only run the analysis script for the final iteration of the loop and process asynchronously to save time
		p1 = Thread(target=PR.execute_sql_script_with_attach, args=(archive_dir / demand_db_name, scripts_dir / "wtf_baseline_analysis.sql", archive_dir / supply_db_name))
		p1.start()
	

