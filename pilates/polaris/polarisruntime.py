import sys
import subprocess
import logging

# We import polarislib functions here that we use elsewhere so that everything from
# polarislib can be run as PR.method_name
from polarislib.utils.database.db_utils import run_sql_file
from polarislib.runs.scenario_file import apply_modification
from polarislib.runs.run_utils import get_latest_polaris_output, merge_csvs
from polarislib.runs.polaris_inputs import PolarisInputs
from polarislib.runs.convergence.convergence_config import ConvergenceConfig
from polarislib.runs.gap_reporting import generate_gap_report
from polarislib.skims import HighwaySkim

# we define our own copy_replace_file in favour of the one from polarislib
from pilates.polaris.file_utilities import copy_replace_file


logger = logging.getLogger("polaris.utils")

def run_polaris_instance(results_dir, exe_name, scenario_file, num_threads, tail_app):
	# subprocess.call([exeName, arguments])
	out_file = open(str(results_dir / 'simulation_out.log'), 'w+')
	err_file = open(str(results_dir / 'simulation_err.log'), 'w+')
	#print ([str(exe_name), str(scenario_file), num_threads])
	proc = subprocess.Popen([str(exe_name), str(scenario_file), str(num_threads)], stdout=out_file, stderr=subprocess.PIPE)
	#output, err = proc.communicate()
	for line in proc.stderr:
		sys.stdout.write(str(line))
		err_file.write(str(line))
	proc.wait()
	out_file.close()
	err_file.close()

	if proc.returncode != 0:
		logger.critical("POLARIS did not execute correctly - {0}".format(proc.returncode))
		return False
	else:
		return True
