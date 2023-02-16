import os
import sys
import subprocess
import yaml
import logging
from logging import config
from pathlib import Path
from pilates.polaris.polarislib import *
from pilates.polaris.modify_scenario import *
from pilates.polaris.file_utilities import *
from pilates.polaris.sqlite_utilities import *


logger = logging.getLogger(__name__)

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
