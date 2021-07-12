import yaml
import docker
import os
import argparse
import logging
import sys

from pilates.activitysim import preprocessor as asim_pre
from pilates.activitysim import postprocessor as asim_post
from pilates.beam import preprocessor as beam_pre
from pilates.beam import postprocessor as beam_post

logging.basicConfig(
    stream=sys.stdout, level=logging.INFO,
    format='%(asctime)s %(name)s - %(levelname)s - %(message)s')


def formatted_print(string, width=50, fill_char='#'):
    print('\n')
    if len(string) + 2 > width:
        width = len(string) + 4
    print(fill_char * width)
    print('{:#^{width}}'.format(' ' + string + ' ', width=width))
    print(fill_char * width, '\n')


def find_latest_beam_iteration(beam_output_dir):
    iter_dirs = []
    for root, dirs, files in os.walk(beam_output_dir):
        for dir in dirs:
            if dir == "ITER":
                iter_dirs += os.path.join(root, dir)
    print(iter_dirs)


if __name__ == '__main__':

    logger = logging.getLogger(__name__)
    # read settings from config file
    with open('settings.yaml') as file:
        settings = yaml.load(file, Loader=yaml.FullLoader)

    s3_io = settings['s3_io']
    activity_demand_image = settings['activity_demand_image']
    travel_model_image = settings['travel_model_image']
    region = settings['region']
    scenario = settings['scenario']
    start_year = settings['start_year']
    end_year = settings['end_year']
    travel_model_freq = settings['travel_model_freq']
    household_sample_size = settings['household_sample_size']
    path_to_skims = settings['path_to_skims']
    beam_config = settings['beam_config']
    beam_local_input_folder = settings['beam_local_input_folder']
    beam_local_output_folder = settings['beam_local_output_folder']
    skim_zone_source_id_col = settings['skim_zone_source_id_col']
    usim_client_data_folder = settings['usim_client_data_folder']
    usim_local_data_folder = settings['usim_local_data_folder']
    asim_bucket = settings['region_to_asim_bucket'][region]
    asim_subdir = settings['region_to_asim_subdir'][region]
    asim_workdir = os.path.join('/activitysim', asim_subdir)
    chunk_size = settings['chunk_size']
    num_processes = settings['num_processes']
    asim_local_input_folder = settings['asim_local_input_folder']
    asim_local_output_folder = settings['asim_local_output_folder']
    beam_subdir = settings['region_to_beam_subdir'][region]
    docker_stdout = settings['docker_stdout']
    pull_latest = settings['pull_latest']
    region_id = settings['region_to_region_id'][region]

    # parse args
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='print docker stdout')
    parser.add_argument(
        '-p', '--pull_latest', action='store_true',
        help='pull latest docker images before running')
    parser.add_argument(
        "-h", "--household_sample_size", action="store",
        help="household sample size")
    args = parser.parse_args()
    if args.verbose:
        docker_stdout = True
    if args.pull_latest:
        pull_latest = True
    if args.household_sample_size:
        household_sample_size = args.household_sample_size

    # prep docker environment
    client = docker.from_env()
    if pull_latest:
        for image in [
            activity_demand_image, travel_model_image]:
            client.images.pull(image)

    # 0. CREATE ONCE INPUT DATA FOR ACTIVITYSIM from H5
    asim_pre.create_asim_data_from_h5(settings, start_year, keys_with_year=False)

    #remember already processed skims
    previous_skims = beam_post.find_produced_skims(beam_local_output_folder)
    logger.info("Found skims from the previous run: %s", previous_skims)

    # run the simulation flow
    for forecast_year in range(start_year, end_year, travel_model_freq):
        logger.info("Processing year %s", forecast_year)

        # 1. PREPROCESS SKIM DATA FOR ACTIVITYSIM
        asim_pre.create_skims_from_beam(asim_local_input_folder, settings)

        # 2. RUN ACTIVITYSIM
        print_str = (
            "Generating activity plans for the year "
            "{0} with {1}".format(
                forecast_year, activity_demand_image))
        formatted_print(print_str)
        formattable_asim_cmd = '-y {0} -h {1} -n {2} -c {3}'
        asim = client.containers.run(
            activity_demand_image, working_dir=asim_workdir,
            volumes={
                os.path.abspath(settings['asim_local_input_folder']): {
                    'bind': os.path.join(asim_workdir, 'data'),
                    'mode': 'rw'},
                os.path.abspath(settings['asim_local_output_folder']): {
                    'bind': os.path.join(asim_workdir, 'output'),
                    'mode': 'rw'}
            },
            command=formattable_asim_cmd.format(
                forecast_year, household_sample_size,
                num_processes, chunk_size
            ),
            stdout=docker_stdout, stderr=True, detach=True, remove=True)
        for log in asim.logs(stream=True, stderr=True, stdout=docker_stdout):
            print(log)

        # 3. COPY ACTIVITYSIM OUTPUT --> URBANSIM INPUT
        print_str = (
            "Generating {0} BEAM input data from "
            "{1} outputs".format(
                forecast_year, activity_demand_image))
        formatted_print(print_str)

        beam_pre.copy_plans_from_asim(settings)

        # 4. RUN BEAM
        abs_beam_input = os.path.abspath(beam_local_input_folder)
        abs_beam_output = os.path.abspath(beam_local_output_folder)
        logger.info("Starting beam container, input: %s, output: %s, config: %s",
                    abs_beam_input, abs_beam_output, beam_config)
        path_to_beam_config = '/app/input/{0}'.format(beam_config)
        client.containers.run(
            travel_model_image,
            volumes={
                abs_beam_input: {
                    'bind': '/app/input',
                    'mode': 'rw'},
                abs_beam_output: {
                    'bind': '/app/output',
                    'mode': 'rw'}},
            command="--config={0}".format(path_to_beam_config),
            stdout=docker_stdout, stderr=True, detach=False, remove=True
        )

        current_skims = beam_post.merge_current_skims(path_to_skims, previous_skims, beam_local_output_folder)
        if current_skims == previous_skims:
            logger.error("BEAM hasn't produced the new skims for some reason. Please check beamLog.out for errors in "
                         "the directory %s", abs_beam_output)
            exit(1)
    logger.info("Finished")
