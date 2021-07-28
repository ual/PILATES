#!/usr/bin/python
# Filename: run_convergence.py

import shutil
# from pathlib import Path
import sys
import os
import subprocess
from shutil import copyfile
from pathlib import Path
import json
import sqlite3
import csv
import traceback
# import regression
# import modify_scenario
import CSV_Utillities
# import init_model
import logging

logger = logging.getLogger(__name__)


# +++++++++++++++++++++++++++++++++++++++
# Run Polaris model for DTA convergence
# +++++++++++++++++++++++++++++++++++++++


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


def copyreplacefile(filename, dest_dir):
    dest_file = Path(dest_dir / filename.name)
    if dest_file.exists():
        os.remove(str(dest_file))
    copyfile(str(filename), str(dest_file))


def execute_sql_script(db_name, script):
    print('Executing Sqlite3 script: %s on database: %s' % (db_name, script))
    with open(str(script), 'r') as sql_file:
        sql_script = sql_file.read()

    db = sqlite3.connect(str(db_name))
    cursor = db.cursor()
    try:
        cursor.executescript(sql_script)
        db.commit()
    except sqlite3.Error as err:
        print('SQLite error: %s' % (' '.join(err.args)))
        print("Exception class is: ", err.__class__)
        print('SQLite traceback: ')
        exc_type, exc_value, exc_tb = sys.exc_info()
        print(traceback.format_exception(exc_type, exc_value, exc_tb))
    db.close()


def dump_table_to_csv(db, table, csv_name, loop):
    db_input = sqlite3.connect(str(db))
    sql3_cursor = db_input.cursor()
    query = 'SELECT * FROM ' + table
    sql3_cursor.execute(query)
    with open(str(csv_name), 'w') as out_csv_file:
        csv_out = csv.writer(out_csv_file, lineterminator='\n')  # gets rid of blank lines - defaults to \r\n
        if loop == 0:
            # write header
            csv_out.writerow([d[0] for d in sql3_cursor.description])
        # write data
        for result in sql3_cursor:
            csv_out.writerow(result)
    db_input.close()


def append_file(src, tar):
    with tar.open("a") as tar_file:  # append mode
        src_file = src.read_text()
        tar_file.write(src_file)
        tar_file.close()


def run_conv(polaris_settings, data_directory):
    database_base_name = polaris_settings.get('db_name')

    # -- SET THE POLARIS RUN INFORMATION
    exe_name = Path(polaris_settings.get('polaris_exe'))
    #data_dir = Path(json_data["data"])
    data_dir = Path(data_directory)
    if not data_dir.exists():
        logger.error(f'\'{data_directory}\' does not exist!')
        sys.exit(-1)
    results_dir = data_dir / polaris_settings.get('results_dir')
    num_threads = polaris_settings.get('num_threads')
    scripts_dir = Path(polaris_settings.get('scripts_dir'))

    # -- SET THE MAIN AND DTA RUN SCENARIO FILES
    scenario_main_init = polaris_settings.get('scenario_main_init')
    scenario_main = polaris_settings.get('scenario_main')

    # -- ENTER THE NUMBER OF MAIN RUNS AND DTA RUNS PER MAIN RUN
    num_abm_runs = polaris_settings.get('num_abm_runs')

    # -- SET THE OUTPUT DIRECTORIES AS SPECIFIED IN THE SCENARIO FILES
    # output_directories = json_data["output_directories"]
    #cloud_backup_path = Path(json_data["cloud_backup_path"])

    # -------------------------------------------------------------------------------------------
    # Do not modify below here
    # -------------------------------------------------------------------------------------------

    # print(data_dir)
    os.chdir(str(data_dir))
    working_dir = Path.cwd()

    if Path("artificial_count.csv").exists():
        os.remove("artificial_count.csv")
    if Path("gap_calculations.csv").exists():
        os.remove("gap_calculations.csv")
    if Path("gap_breakdown.csv").exists():
        os.remove("gap_breakdown.csv")

    # store the original inputs
    supply_db_name = database_base_name + "-Supply.sqlite"
    demand_db_name = database_base_name + "-Demand.sqlite"
    result_db_name = database_base_name + "-Result.sqlite"
    highway_skim_file_name = "highway_skim_file.bin"
    transit_skim_file_name = "transit_skim_file.bin"

    supply_db = working_dir / "backup" / supply_db_name
    demand_db = working_dir / "backup" / demand_db_name
    result_db = working_dir / "backup" / result_db_name
    highway_skim_file = working_dir / "backup" / highway_skim_file_name
    transit_skim_file = working_dir / "backup" / transit_skim_file_name

    output_file = results_dir / "simulation_out.log"

    copyreplacefile(supply_db, working_dir)
    copyreplacefile(demand_db, working_dir)
    copyreplacefile(result_db, working_dir)
    copyreplacefile(highway_skim_file, working_dir)
    copyreplacefile(transit_skim_file, working_dir)
    logger.info(f"Polaris output goes here: {str(output_file)}")

    # Process main ABM run

    # list of result directories
    result_paths = []

    for loop in range(0, int(num_abm_runs)):
        print("\n------------------------------------------------------------------------")
        # Create results Directory if don't exist
        if not results_dir.exists():
            os.mkdir(str(results_dir))
            logger.info(f"Directory:  {results_dir} Created ")
        else:
            logger.info(f"Directory: {results_dir} already exists")

        if loop == 0:
            scenario_file = scenario_main_init
            logger.info(f"Running Polaris SCENARIO_MAIN_INIT instance {loop} - see {results_dir / 'simulation_out.log'}")
        else:
            scenario_file = scenario_main
            # modify_scenario.modify(scenario_main, "time_dependent_routing_weight_factor", 1.0)
            logger.info(f"Running Polaris SCENARIO_MAIN instance #{loop} - see {results_dir / 'simulation_out.log'}")

        arguments = scenario_file + ' ' + num_threads
        logger.info(f'Executing \'{str(exe_name)} {arguments}\'')

        run_polaris_local(results_dir, exe_name, scenario_file, num_threads)

        all_subdirs = [d for d in os.listdir('.') if os.path.isdir(d)]
        latest_subdir = Path(max(all_subdirs, key=os.path.getmtime))

        # standard_dir = 'Regression_test'
        result_paths.append(Path(latest_subdir))

        # move the output files (now that we know where the simulation files were created)
        results_dir_moved = working_dir / latest_subdir / json_data["results_dir"]
        print(f'Moving: {results_dir} to: {results_dir_moved}')
        shutil.move(str(results_dir), str(results_dir_moved))
        # os.rename('./simulation_out.log', simulated_dir + '/simulation_out.log')
        # os.rename('./simulation_err.log', simulated_dir + '/simulation_err.log')

        # copy local results back to the main run directory for the next run
        copyreplacefile(working_dir / latest_subdir / demand_db_name, working_dir)
        copyreplacefile(working_dir / latest_subdir / result_db_name, working_dir)
        if loop > 0:
            copyreplacefile(working_dir / latest_subdir / highway_skim_file_name, working_dir)
            #copyreplacefile(working_dir / latest_subdir / transit_skim_file_name, working_dir)

        # %sqlite3_path%sqlite3.exe "%WORKDIR%\%DB%-Demand.sqlite" < clean_db_after_abm_for_abm.sql
        execute_sql_script(working_dir / demand_db_name, scripts_dir / "clean_db_after_abm_for_abm.sql")

        # ren %WORKDIR%\!out_local!\summary.csv summary_abm_%%S.csv
        latest_demand_db = working_dir / latest_subdir / demand_db_name
        execute_sql_script(latest_demand_db, scripts_dir / "wtf_baseline_analysis_25Per_calibrate.sql")
        execute_sql_script(latest_demand_db, scripts_dir / "gap_calculate.sql")
        # execute_sql_script(working_dir / latest_subdir / demand_db_name, working_dir / "output_to_csv.sql")
        dump_table_to_csv(latest_demand_db, "artificial_count", working_dir / "artificial_count_temp.csv", loop)
        dump_table_to_csv(latest_demand_db, "gap_calculations", working_dir / "gap_calculations_temp.csv", loop)
        dump_table_to_csv(latest_demand_db, "gap_breakdown", working_dir / "gap_breakdown_temp.csv", loop)

        # append temp data to main file
        append_file(working_dir / "artificial_count_temp.csv", working_dir / "artificial_count.csv")
        append_file(working_dir / "gap_calculations_temp.csv", working_dir / "gap_calculations.csv")
        append_file(working_dir / "gap_breakdown_temp.csv", working_dir / "gap_breakdown.csv")

        os.remove(working_dir / "artificial_count_temp.csv")
        os.remove(working_dir / "gap_calculations_temp.csv")
        os.remove(working_dir / "gap_breakdown_temp.csv")

        # if loop > 0:
        #     print(f'Checking convergence on {str(latest_subdir)}')
        #     regression.regression(result_paths[loop - 1].name, result_paths[loop].name)

        # merge in_network data into a composite summary file
        CSV_Utillities.append_column(working_dir / latest_subdir / 'summary.csv', working_dir / 'in_network.csv', loop, 4, str(latest_subdir))


if __name__ == '__main__':
    if len(sys.argv) < 3:
        logger.info(f'Usage {sys.argv[0]} <json_control_file> <data_directory>')
        sys.exit(-1)

    init_model.init_model(sys.argv[2])
    run_conv(sys.argv[1], sys.argv[2])
