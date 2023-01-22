#!/usr/bin/python
# Filename: run_convergence.py

import sys
import os
import subprocess
import glob
from shutil import copyfile
from shutil import copytree
from pathlib import Path
import json
import sqlite3
import csv
import queue
import traceback
import logging

from pilates.polaris.polarislib.run_utils import get_output_dirs, get_output_dir_index, merge_csvs
from pilates.polaris.polarislib.convergence_config import ConvergenceConfig

logger = logging.getLogger(__name__)

def all_subdirs_of(out_name, b='.'):
  result = []
  search_path = '{0}/{1}'.format(b, out_name + '*/model_files')
  result = glob.glob(search_path)
  return result

def get_latest_polaris_output(out_name, data_dir='.'):
	all_subdirs = all_subdirs_of(out_name, data_dir)
	if len(all_subdirs) == 0:
		return None
	else:
		latest_subdir = Path(max(all_subdirs, key=os.path.getctime))
		return latest_subdir.parents[0]

def get_best_polaris_iteration(out_name, abm_runs, data_dir='.'):
	all_subdirs = all_subdirs_of(out_name, data_dir)
	if len(all_subdirs) <= 1:
		return None
	else:
        # get the last batch of abm runs, less the first initialization run
		latest_subdirs = sorted(all_subdirs, key=os.path.getctime)[-(abm_runs-1):]
		return latest_subdir.parents[0]

def get_best_iteration(config: ConvergenceConfig, abm_runs):
    """Determine the best iteration for a convergence run based on the minimum relative gap.

    Args:
        *config* (:obj:`ConvergenceConfig`): Config object defining the convergence run

    Returns:
        *(Path)*: the full path to the sub-directory corresponding to the best iteration
    """

    # select lowest gap iteration
    gaps = merge_csvs(config, "gap_calculations.csv", save_merged=False)

    # verify that subdirs found
    if len(gaps.axes[0]) < abm_runs:
        logger.info(f"Could not find {abm_runs} valid iterations in search directory: {config.data_dir}")
        sys.exit()

    iter_name = gaps.tail(abm_runs)["relative_gap_min0"].idxmin()

    logging.info(f"best iteration = {config.data_dir / iter_name}")
    return config.data_dir / iter_name

	
def copyreplacefile(filename, dest_dir):
	dest_file = Path(dest_dir / filename.name)
	if dest_file.exists():
		os.remove(str(dest_file))
	if filename.exists():
		copyfile(str(filename), str(dest_file))
	else:
		logger.info(f'Copyreplacefile error; filename \'{str(filename)}\' does not exist\'')
	
def archive_polaris_output(output_dir, archive_dir):
	# check if folder already exists
	if not output_dir.exists():
		logger.info(f'archive_polaris_output error; source output directory \'{str(output_dir)}\' does not exist\'')
		return
	if not archive_dir.exists():
		os.mkdir(str(archive_dir))
		logger.info(f"Directory:  {archive_dir} Created ")
	# copy output folder to archive folder
	tgt = archive_dir / os.path.basename(output_dir)
	copy_num = 1
	while tgt.exists():
		tgt = archive_dir / (str(os.path.basename(output_dir)) + '_copy' + str(copy_num))
		copy_num += 1
	copytree(output_dir, tgt)

def append_file(src, tar):
	with tar.open("a") as tar_file:  # append mode
		src_file = src.read_text()
		tar_file.write(src_file)
		tar_file.close()

def append_column(src, tgt, loop, column, header_text):
    all_data = []
    # new_data = []

    with src.open('r') as csv_input:
        reader = csv.reader(csv_input)

        if loop == 0:
            next(reader)  # read header row
            output_row = []
            if loop == 0:
                output_row.append('time')
            output_row.append(header_text)
            all_data.append(output_row)

            for input_row in reader:
                output_row = []
                if loop == 0:
                    output_row.append(input_row[0])
                output_row.append(input_row[column])
                all_data.append(output_row)

            with tgt.open('w') as csv_output:
                writer = csv.writer(csv_output, lineterminator='\n')
                writer.writerows(all_data)
        else:
            new_data_queue = queue.Queue()
            next(reader)  # read header (and ignore)
            for input_row in reader:
                new_data_queue.put(input_row[column])

            with tgt.open('r') as csv_existing:
                existing = csv.reader(csv_existing)
                header = next(existing)  # read header row
                output_row = []
                for h in header:
                    output_row.append(h)
                output_row.append(header_text)
                all_data.append(output_row)
                for existing_row in existing:
                    output_row = []
                    for e in existing_row:
                        output_row.append(e)
                    output_row.append(new_data_queue.get())
                    all_data.append(output_row)

            with tgt.open('w') as csv_output:
                writer = csv.writer(csv_output, lineterminator='\n')
                writer.writerows(all_data)


