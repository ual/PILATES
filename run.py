import yaml
import docker
import os
import argparse
import logging
import sys

from pilates.activitysim import preprocessor as asim_pre
from pilates.activitysim import postprocessor as asim_post
from pilates.urbansim import preprocessor as usim_pre
from pilates.urbansim import postprocessor as usim_post
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

    # parse scenario settings
    image_names = settings['docker_images']
    land_use_model = settings.get('land_use_model', False)
    activity_demand_model = settings.get('activity_demand_model', False)
    travel_model = settings.get('travel_model', False)
    region = settings['region']
    region_id = settings['region_to_region_id'][region]
    scenario = settings['scenario']
    start_year = settings['start_year']
    end_year = settings['end_year']
    travel_model_freq = settings['travel_model_freq']
    skims_fname = settings['skims_fname']

    # parse PILATES defaults
    household_sample_size = settings.get('household_sample_size', 0)
    docker_stdout = settings.get('docker_stdout', False)
    pull_latest = settings.get('pull_latest', False)

    # parse land use settings
    if land_use_model :
        land_use_image = image_names[land_use_model]
        land_use_freq = settings['land_use_freq']
        skim_zone_source_id_col = settings['skim_zone_source_id_col']
        usim_client_data_folder = settings['usim_client_data_folder']
        usim_local_data_folder = settings['usim_local_data_folder']
        formattable_usim_cmd = settings['usim_formattable_command']

    # parse activity demand settings
    if activity_demand_model :
        activity_demand_image = image_names[activity_demand_model]
        asim_subdir = settings['region_to_asim_subdir'][region]
        asim_workdir = os.path.join('/activitysim', asim_subdir)
        chunk_size = settings['chunk_size']
        num_processes = settings['num_processes']
        asim_local_input_folder = settings['asim_local_input_folder']
        asim_local_output_folder = settings['asim_local_output_folder']
        formattable_asim_cmd = settings['asim_formattable_command']

    # parse traffic assignment settings
    if travel_model :
        travel_model_image = image_names[travel_model]
        beam_config = settings['beam_config']
        beam_local_input_folder = settings['beam_local_input_folder']
        beam_local_output_folder = settings['beam_local_output_folder']

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
        for model in [land_use_model, activity_demand_model, travel_model]:
            if model:
                image = image_names[model]
                print('Pulling latest image for {0}'.format(image))
                client.images.pull(image)

    # remember already processed skims
    travel_model_enabled = travel_model
    if travel_model_enabled :
        previous_skims = beam_post.find_produced_skims(beam_local_output_folder)
        logger.info("Found skims from the previous run: %s", previous_skims)

    # run the simulation flow
    for year in range(start_year, end_year, travel_model_freq):

        land_use_enabled = land_use_model
        if land_use_enabled:

            ###########################
            #    FORECAST LAND USE    #
            ###########################

            # 1. PREPARE URBANSIM DATA
            print_str = (
                "Preparing input data for land use development simulation.")
            formatted_print(print_str)

            usim_pre.add_skims_to_model_data(
                settings, region, skim_zone_source_id_col)

            # 2. RUN URBANSIM
            forecast_year = year + travel_model_freq
            print_str = (
                "Simulating land use development from {0} "
                "to {1} with {2}.".format(
                    year, forecast_year, land_use_image.split('/')[1]))
            formatted_print(print_str)
            usim_cmd = formattable_usim_cmd.format(
                region_id, year, forecast_year, land_use_freq)
            usim = client.containers.run(
                land_use_image,
                volumes={
                    os.path.abspath(usim_local_data_folder): {
                        'bind': usim_client_data_folder,
                        'mode': 'rw'},
                },
                command=usim_cmd, stdout=docker_stdout,
                stderr=True, detach=True, remove=True)
            for log in usim.logs(
                    stream=True, stderr=True, stdout=docker_stdout):
                print(log)
        else:
            forecast_year = year

        activity_demand_enabled = activity_demand_model
        if activity_demand_enabled:

            #################################
            #    GENERATE ACTIVITY PLANS    #
            #################################

            # 1. PREPROCESS DATA FOR ACTIVITY DEMAND MODEL
            print_str = "Creating {0} input data from {1} outputs".format(
                activity_demand_image.split('/')[1],
                land_use_image.split('/')[1])
            formatted_print(print_str)
            asim_pre.create_skims_from_beam(asim_local_input_folder, settings)

            if (land_use_enabled) | (forecast_year == start_year):
                asim_pre.create_asim_data_from_h5(
                    settings, forecast_year, keys_with_year=land_use_enabled)

            # 2. GENERATE ACTIVITY PLANS
            print_str = (
                "Generating activity plans for the year "
                "{0} with {1}".format(
                    forecast_year, activity_demand_image))
            formatted_print(print_str)
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
            for log in asim.logs(
                    stream=True, stderr=True, stdout=docker_stdout):
                print(log)

            # 3. COPY ACTIVITY DEMAND OUTPUTS --> LAND USE INPUTS

            # If generating activities for the base year, don't overwrite
            # urbansim input data. This is usually only the case for warm
            # starts or debugging. Otherwise we want to set up urbansim for
            # the next simulation iteration
            if (land_use_enabled) & (forecast_year != start_year):
                print_str = (
                    "Generating {0} {1} input data from "
                    "{2} outputs".format(
                        forecast_year, land_use_model, activity_demand_model))
                formatted_print(print_str)
                asim_post.create_next_iter_inputs(settings, forecast_year)

            # 4. COPY ACTIVITY DEMAND OUTPUTS --> TRAFFIC ASSIGNMENT INPUTS
            print_str = (
                "Generating {0} {1} input data from "
                "{2} outputs".format(
                    forecast_year, travel_model, activity_demand_model))
            formatted_print(print_str)
            beam_pre.copy_plans_from_asim(settings)

        else:

            # CONVERT LAND USE OUTPUTS TO NEXT ITERATION INPUTS
            usim_post.create_next_iter_usim_data(settings, year)

        travel_model_enabled = travel_model
        if travel_model_enabled:

            #################################
            #    RUN TRAFFIC ASSIGNMENT    #
            #################################

            # 4. RUN BEAM
            abs_beam_input = os.path.abspath(beam_local_input_folder)
            abs_beam_output = os.path.abspath(beam_local_output_folder)
            logger.info(
                "Starting beam container, input: %s, output: %s, config: %s",
                abs_beam_input, abs_beam_output, beam_config)
            path_to_beam_config = '/app/input/{0}/{1}'.format(
                region, beam_config)
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
            path_to_skims = os.path.join(os.path.abspath(
                beam_local_output_folder), skims_fname)
            current_skims = beam_post.merge_current_skims(
                path_to_skims, previous_skims, beam_local_output_folder)
            if current_skims == previous_skims:
                logger.error(
                    "BEAM hasn't produced the new skims for some reason. "
                    "Please check beamLog.out for errors in the directory %s",
                    abs_beam_output)
                exit(1)

        logger.info("Finished")
