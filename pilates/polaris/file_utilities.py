#!/usr/bin/python
import os,csv,queue,logging
from shutil import copyfile, copytree
from pathlib import Path
from os.path import join
import polarisruntime as PR

logger = logging.getLogger("polaris.utils")

def all_subdirs_of(out_name, base_dir='.'):
  model_file_dirs = Path(base_dir).glob(join(f"{out_name}*", 'model_files'))
  return [e.parent for e in model_file_dirs]

def get_latest_polaris_output(out_name, data_dir='.'):
	all_subdirs = all_subdirs_of(out_name, data_dir)
	if len(all_subdirs) == 0:
		return None
	else:
		return Path(max(all_subdirs, key=os.path.getctime))

def get_best_iteration(config: PR.ConvergenceConfig, abm_runs):
    """Determine the best iteration for a convergence run based on the minimum relative gap.

    Args:
        *config* (:obj:`ConvergenceConfig`): Config object defining the convergence run

    Returns:
        *(Path)*: the full path to the sub-directory corresponding to the best iteration
    """

    # select lowest gap iteration
    gaps = PR.merge_csvs(config, "gap_calculations.csv", save_merged=False)

    # verify that subdirs found
    if len(gaps.axes[0]) < abm_runs:
        logger.info(f"Could not find {abm_runs} valid iterations in search directory: {config.data_dir}")
        sys.exit()

    # ignore the first three abm runs to allow for convergence...
    iter_name = gaps.tail(abm_runs-3)["relative_gap_min0"].idxmin()

    logging.info(f"best iteration = {config.data_dir / iter_name}")
    return config.data_dir / iter_name

def delete_unneeded_results(config: PR.ConvergenceConfig):
    subdirs = all_subdirs_of(config.db_name, config.data_dir)
    logger.info("Deleting sub-dirs:")
    for d in subdirs:
        logger.info(f"  -> {d}")
        for r in d.glob("*-Result.sqlite"):
            r.unlink(missing_ok=True)

def copy_replace_file(filename, dest_dir):
    dest_file = Path(dest_dir) / Path(filename).name
    logger.info(f"CopyReplace from {filename}")
    logger.info(f"CopyReplace to   {dest_file}")
    dest_file.unlink(missing_ok=True)
    copyfile(str(filename), str(dest_file))


def archive_polaris_output(output_dir, archive_dir):
	# check if folder already exists
	if not output_dir.exists():
		logger.info(f'archive_polaris_output error; source output directory \'{output_dir}\' does not exist\'')
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


if __name__ == "__main__":
    model_dir = "/lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/runs/3_L4_cacc_ref/austin"
    conf = PR.ConvergenceConfig(model_dir, "campo")
    delete_unneeded_results(conf)