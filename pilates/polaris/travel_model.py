import os
import sys
import subprocess
import yaml
import shutil
from pilates.polaris.modify_scenario import apply_modification
import pilates.polaris.preprocessor as preprocessor
import pilates.polaris.postprocessor as postprocessor
import logging
import glob
import fnmatch
import pilates.polaris.polarisruntime as PR
from pathlib import Path
from os.path import join, abspath
from threading import Thread

from pilates.polaris.polarislib.convergence_config import ConvergenceConfig
from pilates.polaris.polarislib.gap_reporting import generate_gap_report
from pilates.polaris.file_utilities import get_best_iteration

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

def run_polaris(forecast_year, settings, warm_start=False):

	logger.info('**** RUNNING POLARIS FOR FORECAST YEAR {0}, WARM START MODE = {1}'.format(forecast_year, warm_start))
	pilates_data_dir = Path(abspath(settings['data_folder']))
	pilates_src_dir = settings['pilates_src_dir']

	# read data specific settings from config file in the data dir
	with open(pilates_data_dir / 'pilates' / 'polaris' / 'polaris_settings.yaml') as file:
		polaris_settings = yaml.load(file, Loader=yaml.FullLoader)
	model_dir = pilates_data_dir / "austin" # TODO: No hardcode
	backup_dir = pilates_data_dir / "backup"
	scripts_dir = pilates_src_dir / "pilates" / 'polaris' / "conv_scripts"
	db_name = polaris_settings['db_name']
	out_name = polaris_settings['out_name']
	scenario_init_file = polaris_settings['scenario_main_init']
	scenario_main_file = polaris_settings['scenario_main']
	vehicle_file_base = polaris_settings.get('vehicle_file_basename', 'vehicle_distribution')
	vehicle_file_fleet_base = polaris_settings.get('fleet_vehicle_file_basename', 'vehicle_fleet_distribution')
	num_threads = polaris_settings.get('num_threads', 6)
	num_abm_runs = polaris_settings['num_abm_runs']
	block_loc_file_name = polaris_settings['block_loc_file_name']
	population_scale_factor = polaris_settings['population_scale_factor']
	archive_dir = polaris_settings.get('archive_dir')
	db_supply = f"{model_dir}/{db_name}-Supply.sqlite"
	db_demand = f"{model_dir}/{db_name}-Demand.sqlite"
	block_loc_file = "{0}/{1}".format(str(model_dir), block_loc_file_name)
	vot_level = polaris_settings.get('vot_level')

	# Things which come from the main settings.yaml
	polaris_exe = settings['polaris_exe']
	usim_output_dir = pilates_data_dir / settings['usim_local_data_folder']

	# store the original inputs
	supply_db_name = db_name + "-Supply.sqlite"
	demand_db_name = db_name + "-Demand.sqlite"
	result_db_name = db_name + "-Result.sqlite"
	highway_skim_file_name = "highway_skim_file.bin"
	transit_skim_file_name = "transit_skim_file.bin"

	pwd = os.getcwd()
	os.chdir(model_dir)

	#check if warm start and initialize changes
	if warm_start:
		num_abm_runs = 1

		# start with fresh demand database from backup (only for warm start)
		PR.copyreplacefile(backup_dir / demand_db_name, model_dir)

		# # load the urbansim population for the init run
		preprocessor.preprocess_usim_for_polaris(forecast_year, usim_output_dir, block_loc_file, db_supply, db_demand, 1.0, settings)

		# update the vehicles table with new costs
		if forecast_year:
			veh_script = "vehicle_operating_cost_" + str(forecast_year) + ".sql"
			PR.execute_sql_script(model_dir / demand_db_name, model_dir / veh_script)

	fail_count = 0
	loop = 0

	while loop < int(num_abm_runs):
		if forecast_year:
			scenario_file = update_scenario_file(scenario_main_file, forecast_year)
			# set vehicle distribution file name based on forecast year
			veh_file_name = vehicle_file_base + '_{0}.txt'.format(forecast_year)
			fleet_veh_file_name = vehicle_file_fleet_base + '_{0}.txt'.format(forecast_year)
		else:
			scenario_file = scenario_init_file
			veh_file_name = vehicle_file_base + '.txt'.format(forecast_year)
			fleet_veh_file_name = vehicle_file_fleet_base + '.txt'.format(forecast_year)

		mods = {}

		if loop == 0:
			print(f"forecast year: {forecast_year}")

			mods["time_dependent_routing_weight_factor"] = 1.0
			mods["read_population_from_database"] = True

			# set warm_start specific settings (that are also modified by loop...)
			if warm_start:
				mods["percent_to_synthesize"] = 1.0
				mods["read_population_from_urbansim"] = True
				mods["warm_start_mode"] = True
				mods["time_dependent_routing"] = False
				mods["multimodal_routing"] = False
				mods["use_tnc_system"] = False
				mods["output_moe_for_assignment_interval"] = False
				mods["output_link_moe_for_simulation_interval" ] = False
				mods["output_link_moe_for_assignment_interval" ] = False
				mods["output_turn_movement_moe_for_assignment_interval" ] = False
				mods["output_turn_movement_moe_for_simulation_interval" ] = False
				mods["output_network_moe_for_simulation_interval" ] = False
				mods["write_skim_tables" ] = False
				mods["write_vehicle_trajectory" ] = False
				mods["write_transit_trajectory" ] = False
				mods["read_trip_factors"] = { "External": 1.0 }
				mods["traffic_scale_factor"] = 1.0
			else:
				mods["percent_to_synthesize"] = population_scale_factor
				mods["read_population_from_urbansim"] = False
				mods["warm_start_mode"] = False
				mods["time_dependent_routing"] = False
				mods["tnc_feedback"] = False
				mods["multimodal_routing"] = True
				mods["use_tnc_system"] = True
				mods["output_link_moe_for_assignment_interval" ] = True
				mods["output_turn_movement_moe_for_assignment_interval" ] = True
				mods["write_skim_tables" ] = True
				mods["write_vehicle_trajectory" ] = True
				# mods["demand_reduction_factor"] = population_scale_factor)
				mods["read_trip_factors"] = { "External": population_scale_factor }
				mods["traffic_scale_factor"] = population_scale_factor

			if warm_start and not forecast_year:
				mods["replan_workplaces"] = True
			else:
				mods["replan_workplaces"] = False
		else:
			mods["time_dependent_routing_weight_factor"] = 1.0/int(loop)
			mods["percent_to_synthesize"] = 1.0
			mods["traffic_scale_factor"] = population_scale_factor
			mods["read_trip_factors"] = { "External": 1.0 }
			mods["read_population_from_urbansim"] = False
			mods["read_population_from_database"] = True
			mods["replan_workplaces"] = False

		mods["vehicle_distribution_file_name"] = veh_file_name
		mods["fleet_vehicle_distribution_file_name"] = fleet_veh_file_name

		print(f"Scenario file: {scenario_file}")
		print(f"mods         : {mods}")
		sc_file = apply_modification(scenario_file, mods)

		# run executable
		logger.info(f'Executing \'{polaris_exe} {sc_file} {num_threads}\'')
		success = PR.run_polaris_instance(model_dir, polaris_exe, sc_file, num_threads, None)

		# get output directory and write files into working dir for next run
		output_dir = PR.get_latest_polaris_output(out_name, model_dir)

		if success:
			PR.copyreplacefile(output_dir / demand_db_name, model_dir)
			PR.execute_sql_script(model_dir / demand_db_name, scripts_dir / "clean_db_after_abm_for_abm.sql")
			# skip all of the file storage and analysis for warm start runs - only need the demand file
			if not warm_start:
				fail_count = 0
				# copy the network outputs back to main data directory
				PR.copyreplacefile(output_dir / result_db_name, model_dir)
				PR.copyreplacefile(output_dir / highway_skim_file_name, model_dir)
				PR.copyreplacefile(model_dir / supply_db_name, output_dir)
				# JA- 1/22/23 - generate gap reports - replace with actual polarislib at some point...
				generate_gap_report(None, output_dir)
			loop += 1
		else:
			fail_count += 1
			if fail_count >= 0:
				logger.info(f"POLARIS crashed {fail_count} times in a row")
				sys.exit()
			else:
				logger.info(f"Deleting failed results directory for attempt: {loop}")
				shutil.rmtree(output_dir)


	# find the latest output
	#output_dir = PR.get_latest_polaris_output(out_name, model_dir)

	# db_supply = "{0}/{1}-Supply.sqlite".format(output_dir, db_name)
	# db_demand = "{0}/{1}-Demand.sqlite".format(output_dir, db_name)
	# db_result =  "{0}/{1}-Result.sqlite".format(output_dir, db_name)
	# auto_skim = "{0}/{1}".format(output_dir, polaris_settings.get('auto_skim_file'))
	# transit_skim = "{0}/{1}".format(output_dir, polaris_settings.get('transit_skim_file'))
	os.chdir(pwd)

	# postprocessor.generate_polaris_skims_for_usim( 'pilates/polaris/data', db_name, db_supply, db_demand, db_result, auto_skim, transit_skim, vot_level)

	if warm_start:
		postprocessor.update_usim_after_polaris(forecast_year, usim_output_dir, db_demand, settings)
		# store the updated full population demand database for the next warm start round
		PR.copyreplacefile(model_dir / demand_db_name, backup_dir)
	else:
		# fing the best output by gap
		conf = ConvergenceConfig(model_dir,db_name)
		output_dir = get_best_iteration(conf, num_abm_runs)

		archive_dir = postprocessor.archive_polaris_output(db_name, forecast_year, output_dir, model_dir)
		postprocessor.archive_and_generate_usim_skims(pilates_data_dir, forecast_year, db_name, output_dir, vot_level)
		# only run the analysis script for the final iteration of the loop and process asynchronously to save time
		p1 = Thread(target=PR.execute_sql_script_with_attach, args=(archive_dir / demand_db_name, scripts_dir / "wtf_baseline_analysis.sql", archive_dir / supply_db_name))
		p1.start()


