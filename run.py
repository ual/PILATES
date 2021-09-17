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
    string = string.upper()
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


def parse_args_and_settings(settings_file='settings.yaml'):

    # read settings from config file
    with open(settings_file) as file:
        settings = yaml.load(file, Loader=yaml.FullLoader)

    # parse command-line args
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        '-v', '--verbose', action='store_true', help='print docker stdout')
    parser.add_argument(
        '-p', '--pull_latest', action='store_true',
        help='pull latest docker images before running')
    parser.add_argument(
        "-h", "--household_sample_size", action="store",
        help="household sample size")
    parser.add_argument(
        "-s", "--static_skims", action="store_true",
        help="bypass traffic assignment, use same skims for every run.")
    parser.add_argument(
        "-w", "--warm_start_skims", action="store_true",
        help="generate full activity plans for the base year only.")
    args = parser.parse_args()

    # command-line only settings:
    settings.update({
        'static_skims': args.static_skims,
        'warm_start_skims': args.warm_start_skims})

    # override .yaml settings with command-line values if command-line
    # values are not False/None
    if args.verbose:
        settings.update({'docker_stdout': args.verbose})
    if args.pull_latest:
        settings.update({'pull_latest': args.pull_latest})
    if args.household_sample_size:
        settings.update({
            'household_sample_size': args.household_sample_size})

    # remember already processed skims
    previous_skims = beam_post.find_produced_skims(
        settings['beam_local_output_folder'])
    settings.update({'previous_skims': previous_skims})
    if previous_skims:
        logger.info("Found skims from the previous run: %s", previous_skims)

    # turn models on or off
    land_use_enabled = ((
        settings.get('land_use_model', False)) and (
        not settings.get('warm_start_skims')))
    activity_demand_enabled = settings.get('activity_demand_model', False)
    traffic_assignment_enabled = ((
        settings.get('travel_model', False)) and (
        not settings['static_skims']))
    settings.update({
        'land_use_enabled': land_use_enabled,
        'activity_demand_enabled': activity_demand_enabled,
        'traffic_assignment_enabled': traffic_assignment_enabled})

    return settings


def get_base_asim_cmd(settings):
    formattable_asim_cmd = settings['asim_formattable_command']
    household_sample_size = settings.get('household_sample_size', 0)
    num_processes = settings.get('num_processes', 4)
    chunk_size = settings.get('chunk_size', 0)  # default no chunking
    base_asim_cmd = formattable_asim_cmd.format(
        household_sample_size, num_processes, chunk_size)
    return base_asim_cmd


def get_asim_docker_vols(settings):
    region = settings['region']
    asim_subdir = settings['region_to_asim_subdir'][region]
    asim_remote_workdir = os.path.join('/activitysim', asim_subdir)
    asim_local_input_folder = os.path.abspath(
        settings['asim_local_input_folder'])
    asim_local_output_folder = os.path.abspath(
        settings['asim_local_output_folder'])
    asim_remote_input_folder = os.path.join(
        asim_remote_workdir, 'data')
    asim_remote_output_folder = os.path.join(
        asim_remote_workdir, 'output')
    asim_docker_vols = {
        asim_local_input_folder: {
            'bind': asim_remote_input_folder,
            'mode': 'rw'},
        asim_local_output_folder: {
            'bind': asim_remote_output_folder,
            'mode': 'rw'}}
    return asim_docker_vols


def get_usim_docker_vols(settings):
    usim_remote_data_folder = settings['usim_client_data_folder']
    usim_local_data_folder = os.path.abspath(
        settings['usim_local_data_folder'])
    usim_docker_vols = {
        usim_local_data_folder: {
            'bind': usim_remote_data_folder,
            'mode': 'rw'}}
    return usim_docker_vols


def get_usim_cmd(settings, year, forecast_year):
    region = settings['region']
    region_id = settings['region_to_region_id'][region]
    land_use_freq = settings['land_use_freq']
    formattable_usim_cmd = settings['usim_formattable_command']
    usim_cmd = formattable_usim_cmd.format(
        region_id, year, forecast_year, land_use_freq)
    return usim_cmd


def warm_start_activities(settings, year, client):
    """
    Run ActivitySim to update UrbanSim inputs with long-term
    choices it needs: workplace location, school location, and
    auto ownership.
    """

    # 1. PARSE SETTINGS
    activity_demand_model = settings['activity_demand_model']
    land_use_model = settings['land_use_model']
    travel_model = settings['travel_model']
    image_names = settings['docker_images']
    activity_demand_image = image_names[activity_demand_model]
    region = settings['region']
    asim_subdir = settings['region_to_asim_subdir'][region]
    asim_workdir = os.path.join('/activitysim', asim_subdir)
    asim_docker_vols = get_asim_docker_vols(settings)
    base_asim_cmd = get_base_asim_cmd(settings)
    docker_stdout = settings.get('docker_stdout', False)

    print_str = "Initializing {0} warm start sequence".format(
        activity_demand_model)
    formatted_print(print_str)

    # 2. CREATE DATA FROM BASE YEAR SKIMS AND URBANSIM INPUTS

    # skims
    logger.info("Creating {0} skims from {1}".format(
        activity_demand_model,
        travel_model).upper())
    asim_pre.create_skims_from_beam(settings)

    # data tables
    logger.info("Creating {0} input data from {1} outputs".format(
        activity_demand_model,
        land_use_model).upper())
    asim_pre.create_asim_data_from_h5(settings, year, warm_start=True)

    # 3. RUN ACTIVITYSIM IN WARM START MODE
    logger.info("Running {0} in warm start mode".format(
        activity_demand_model).upper())
    ws_asim_cmd = base_asim_cmd + ' -w'  # warm start flag

    asim = client.containers.run(
        activity_demand_image,
        working_dir=asim_workdir,
        volumes=asim_docker_vols,
        command=ws_asim_cmd,
        stdout=docker_stdout,
        stderr=True,
        detach=True)
    for log in asim.logs(stream=True, stderr=True, stdout=docker_stdout):
        print(log)

    # 4. UPDATE URBANSIM BASE YEAR INPUT DATA
    logger.info((
        "Appending warm start activities/choices to "
        " {0} base year input data").format(land_use_model).upper())
    asim_post.update_usim_inputs_after_warm_start(settings)

    # 5. CLEANUP
    asim.remove()
    logger.info('Done!')

    return


def forecast_land_use(settings, year, forecast_year, client):

    # 1. PARSE SETTINGS
    image_names = settings['docker_images']
    land_use_model = settings.get('land_use_model', False)
    land_use_image = image_names[land_use_model]
    usim_docker_vols = get_usim_docker_vols(settings)
    usim_cmd = get_usim_cmd(settings, year, forecast_year)
    docker_stdout = settings.get('docker_stdout', False)

    # 2. PREPARE URBANSIM DATA
    print_str = (
        "Preparing {0} input data for land use development simulation.".format(
            year))
    formatted_print(print_str)
    usim_pre.add_skims_to_model_data(settings)

    # 3. RUN URBANSIM
    print_str = (
        "Simulating land use development from {0} "
        "to {1} with {2}.".format(
            year, forecast_year, land_use_model))
    formatted_print(print_str)
    usim = client.containers.run(
        land_use_image,
        volumes=usim_docker_vols,
        command=usim_cmd,
        stdout=docker_stdout,
        stderr=True,
        detach=True)
    for log in usim.logs(
            stream=True, stderr=True, stdout=docker_stdout):
        print(log)

    # 4. CLEAN UP
    usim.remove()

    logger.info('Done!')

    return


def generate_activity_plans(
        settings, year, forecast_year, client, warm_start=False,
        overwrite_skims=True):
    """
    Parameters
    ----------
    year : int
        Start year for the simulation iteration.
    forecast_year : int
        Simulation year for which activities are generated. If `forecast_year`
        is the start year of the whole simulation, then we are probably
        generating warm start activities based on the base year input data in
        order to generate "warm start" skims.
    """

    # 1. PARSE SETTINGS
    activity_demand_model = settings['activity_demand_model']
    land_use_model = settings['land_use_model']
    image_names = settings['docker_images']
    activity_demand_image = image_names[activity_demand_model]
    region = settings['region']
    asim_subdir = settings['region_to_asim_subdir'][region]
    asim_workdir = os.path.join('/activitysim', asim_subdir)
    asim_docker_vols = get_asim_docker_vols(settings)
    base_asim_cmd = get_base_asim_cmd(settings)
    docker_stdout = settings.get('docker_stdout', False)

    # If this is the first iteration, skims should only exist because
    # they were created during the warm start activities step. The skims
    # haven't been updated since then so we don't need to re-create them.
    if year == settings['start_year']:
        overwrite_skims = False

    # 2. PREPROCESS DATA FOR ACTIVITY DEMAND MODEL
    print_str = "Creating {0} inputs data from {1} outputs".format(
        activity_demand_model,
        land_use_model)
    formatted_print(print_str)
    asim_pre.create_skims_from_beam(settings, overwrite=overwrite_skims)
    asim_pre.create_asim_data_from_h5(
        settings, year=forecast_year, warm_start=warm_start)

    # 3. GENERATE ACTIVITY PLANS
    print_str = (
        "Generating activity plans for the year "
        "{0} with {1}".format(
            year, activity_demand_model))
    formatted_print(print_str)
    asim = client.containers.run(
        activity_demand_image,
        working_dir=asim_workdir,
        volumes=asim_docker_vols,
        command=base_asim_cmd,
        stdout=docker_stdout,
        stderr=True,
        detach=True)
    for log in asim.logs(
            stream=True, stderr=True, stdout=docker_stdout):
        print(log)

    # 4. COPY ACTIVITY DEMAND OUTPUTS --> LAND USE INPUTS
    # If generating activities for the base year (i.e. warm start),
    # then we don't want to overwrite urbansim input data. Otherwise
    # we want to set up urbansim for the next simulation iteration
    if (settings['land_use_enabled']) and (not warm_start):
        print_str = (
            "Generating {0} {1} input data from "
            "{2} outputs".format(
                forecast_year, land_use_model, activity_demand_model))
        formatted_print(print_str)
        asim_post.create_next_iter_inputs(settings, year, forecast_year)

    # 6. CLEANUP
    asim.remove()

    logger.info('Done!')

    return


def run_traffic_assignment(settings, year, client):
    """
    This step will run the traffic simulation platform and
    generate new skims with updated congested travel times.
    """

    # 1. PARSE SETTINGS
    beam_config = settings['beam_config']
    region = settings['region']
    path_to_beam_config = '/app/input/{0}/{1}'.format(
        region, beam_config)
    beam_local_input_folder = settings['beam_local_input_folder']
    abs_beam_input = os.path.abspath(beam_local_input_folder)
    beam_local_output_folder = settings['beam_local_output_folder']
    abs_beam_output = os.path.abspath(beam_local_output_folder)
    image_names = settings['docker_images']
    travel_model = settings.get('travel_model', False)
    travel_model_image = image_names[travel_model]
    activity_demand_model = settings.get('activity_demand_model', False)
    docker_stdout = settings['docker_stdout']
    skims_fname = settings['skims_fname']
    previous_skims = settings['previous_skims']

    # 2. COPY ACTIVITY DEMAND OUTPUTS --> TRAFFIC ASSIGNMENT INPUTS
    if settings['traffic_assignment_enabled']:
        print_str = (
            "Generating {0} {1} input data from "
            "{2} outputs".format(
                year, travel_model, activity_demand_model))
        formatted_print(print_str)
        beam_pre.copy_plans_from_asim(settings)

    # 3. RUN BEAM
    logger.info(
        "Starting beam container, input: %s, output: %s, config: %s",
        abs_beam_input, abs_beam_output, beam_config)
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

    # 4. POSTPROCESS
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

    return


def initialize_docker_client(settings):

    land_use_model = settings.get('land_use_model', False)
    activity_demand_model = settings.get('activity_demand_model', False)
    travel_model = settings.get('travel_model', False)
    models = [land_use_model, activity_demand_model, travel_model]
    image_names = settings['docker_images']
    pull_latest = settings.get('pull_latest', False)
    client = docker.from_env()
    if pull_latest:
        for model in models:
            if model:
                image = image_names[model]
                print('Pulling latest image for {0}'.format(image))
                client.images.pull(image)

    return client


if __name__ == '__main__':

    logger = logging.getLogger(__name__)

    #########################################
    #  PREPARE PILATES RUNTIME ENVIRONMENT  #
    #########################################

    # load args and settings
    settings = parse_args_and_settings()

    # parse scenario settings
    start_year = settings['start_year']
    end_year = settings['end_year']
    travel_model_freq = settings.get('travel_model_freq', 1)
    warm_start_skims = settings['warm_start_skims']
    static_skims = settings['static_skims']
    land_use_enabled = settings['land_use_enabled']
    activity_demand_enabled = settings['activity_demand_enabled']
    traffic_assignment_enabled = settings['traffic_assignment_enabled']

    if warm_start_skims:
        formatted_print('RUNNING PILATES IN "WARM START SKIMS" MODE')
        logger.info('Generating activity plans for the base year only.')
    elif static_skims:
        formatted_print('RUNNING PILATES IN "STATIC SKIMS" MODE')
        logger.info('Using the same set of skims for every iteration.')

    # start docker client
    client = initialize_docker_client(settings)

    #################################
    #  RUN THE SIMULATION WORKFLOW  #
    #################################

    for year in range(start_year, end_year, travel_model_freq):

        # 1. FORECAST LAND USE
        if land_use_enabled:

            # 1a. IF START YEAR, WARM START MANDATORY ACTIVITIES
            if year == start_year:
                warm_start_activities(settings, year, client)

            forecast_year = year + travel_model_freq
            forecast_land_use(settings, year, forecast_year, client)

        else:
            forecast_year = year

        # 2. GENERATE ACTIVITIES
        if activity_demand_enabled:

            # If the forecast year is the same as the base year of this
            # iteration, then land use forecasting have not been run. In this
            # case we have to read from the land use *inputs* because no
            # *outputs* have been generated yet. This is usually only the case
            # for generating "warm start" skims, so we treat it the same even
            # if the "warm_start_skims" setting was not set to True at runtime
            if forecast_year == year:
                warm_start_skims = True

            generate_activity_plans(
                settings, year, forecast_year, client, warm_start_skims)

        else:

            # If not generating activities with a separate ABM (e.g.
            # ActivitySim), then we need to create the next iteration of land
            # use data directly from the last set of land use outputs.
            usim_post.create_next_iter_usim_data(settings, year)

        # 3. UPDATE CONGESTED TRAVEL TIMES
        if traffic_assignment_enabled:
            run_traffic_assignment(settings, forecast_year, client)

    logger.info("Finished")
