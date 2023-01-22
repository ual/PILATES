import os
import logging
import sys
import pandas as pd
from os.path import join
from pathlib import Path

from pilates.polaris.polarislib.convergence_config import ConvergenceConfig
from pilates.polaris.polarislib.run_utils import get_output_dirs, get_output_dir_index, merge_csvs
from pilates.polaris.polarislib.db_utils import has_table, table_to_csv, run_sql_file, commit_and_close



def load_gaps(gap_path):
    try:
        df = pd.read_csv(gap_path, index_col=False)
        return df
    except:
        print(f"Gap path {gap_path} not found.")
        return None


def generate_gap_report(config: ConvergenceConfig, output_dir):
    generate_gap_report_for_dir(output_dir)
    #generate_summary_gap_report(config)


def generate_all_gap_reports(config: ConvergenceConfig):
    folder_list = sorted(get_output_dirs(config), key=get_output_dir_index)
    [generate_gap_report_for_dir(folder) for folder in folder_list]
    generate_summary_gap_report(config)


def generate_summary_gap_report(config):
    merge_csvs(config, "gap_calculations.csv")
    merge_csvs(config, "gap_breakdown.csv")


def generate_gap_report_for_dir(dir_path):
    """creates the csvs if they are not existent. Untar file if needed and run the query"""

    logging.info(f"            - {dir_path}")
    files_in_directory = os.listdir(dir_path)
    if "gap_calculations.csv" in files_in_directory and "gap_breakdown.csv" in files_in_directory:
        return

    # if we are here, either we need to open the demand database
    has_demand = False
    demand_file = None

    is_tar = False
    tar_file_path = None

    for file in files_in_directory:
        if file.lower().endswith("demand.sqlite"):
            has_demand = True
            demand_file = dir_path / file
            break

        if file.lower().endswith("demand.sqlite.tar.gz"):
            tar_file_path = dir_path / file

    if has_demand is False and tar_file_path is not None:
        is_tar = True

    if is_tar:
        raise "blah"
        # tar_file = tarfile.TarFile.open(tar_file_path, mode='r')
        # members = tar_file.getmembers()
        # tar_file.extractall(Path(self.project_dir, dir_path))
        #
        # files_in_directory = os.listdir(Path(self.project_dir, dir_path))
        # for file in files_in_directory:
        #     if file.lower().endswith("demand.sqlite"):
        #         has_demand = True
        #         demand_file = file
        #         break
        #
        # if has_demand is False:
        #     print("couldn't generate gap for ", dir_path)
        #     return False

    if is_tar is False and has_demand is False:
        print("couldn't generate gap for ", dir_path)
        return False

    try:
        sql_dir = Path(__file__).parent / "sql"
        with commit_and_close((demand_file)) as conn:
            if has_table(conn, "gap_calculations"):
                conn.execute("drop table if exists gap_calculations")
            if has_table(conn, "gap_breakdown"):
                conn.execute("drop table if exists gap_breakdown")
            run_sql_file(sql_dir / "gap_calculations.sql", conn)
            run_sql_file(sql_dir / "gap_breakdown.sql", conn)

            table_to_csv(conn, "gap_calculations", join(dir_path, "gap_calculations.csv"))
            table_to_csv(conn, "gap_calculations_binned", join(dir_path, "gap_calculations_binned.csv"))
            table_to_csv(conn, "gap_breakdown", join(dir_path, "gap_breakdown.csv"))
            table_to_csv(conn, "gap_breakdown_binned", join(dir_path, "gap_breakdown_binned.csv"))

    except Exception as e:
        print("error on directory", dir_path)
        print(sys.exc_info())
        raise e

    if is_tar:
        os.remove(demand_file)

    return True


# def is_run_directory(self, dir_path):
#     return any([f.startswith('summary') for f in os.listdir((dir_path))])


# if __name__ == '__main__':
#     # path = sys.argv[1]
#     # folder = pathlib.Path(path)

# gap_reporting = GapReporting(Path("D:\\PolarisModels\\bloomington-model-git\\"))
# gap_reporting.process_directory(Path("D:\\PolarisModels\\bloomington-model-git\\bloomington_abm4"))
# gap_reporting.process(False)
