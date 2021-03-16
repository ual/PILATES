import yaml
import docker
import os
import s3fs
import argparse
import logging
import sys

from pilates.activitysim.preprocessor import create_skims_from_beam

logging.basicConfig(
    stream=sys.stdout, level=logging.DEBUG,
    format='%(asctime)s - %(levelname)s - %(message)s')


def formatted_print(string, width=50, fill_char='#'):
    print('\n')
    if len(string) + 2 > width:
        width = len(string) + 4
    print(fill_char * width)
    print('{:#^{width}}'.format(' ' + string + ' ', width=width))
    print(fill_char * width, '\n')


if __name__ == '__main__':

    # read settings from config file
    with open('settings.yaml') as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)

    land_use_image = configs['land_use_image']
    activity_demand_image = configs['activity_demand_image']
    travel_model_image = configs['travel_model_image']
    region = configs['region']
    scenario = configs['scenario']
    start_year = configs['start_year']
    end_year = configs['end_year']
    land_use_freq = configs['land_use_freq']
    travel_model_freq = configs['travel_model_freq']
    household_sample_size = configs['household_sample_size']
    path_to_beam_skims = configs['path_to_beam_skims']
    beam_local_config = configs['beam_local_config']
    beam_local_input_folder_name = configs['beam_local_input_folder_name']
    beam_local_output_folder_name = configs['beam_local_output_folder_name']
    usim_bucket = configs['region_to_usim_bucket'][region]
    asim_bucket = configs['region_to_asim_bucket'][region]
    asim_subdir = configs['region_to_asim_subdir'][region]
    asim_workdir = os.path.join('/activitysim', asim_subdir)
    beam_subdir = configs['region_to_beam_subdir'][region]
    docker_stdout = configs['docker_stdout']
    pull_latest = configs['pull_latest']

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
                land_use_image, activity_demand_image, travel_model_image]:
            client.images.pull(image)

    # prep aws s3 client
    s3fs.S3FileSystem.read_timeout = 84600
    s3 = s3fs.S3FileSystem(config_kwargs={'read_timeout': 86400})
    formattable_s3_path = '{bucket}/{io}/{scenario}/{year}/{fname}'

    for year in range(start_year, end_year, travel_model_freq):

        forecast_year = year + travel_model_freq

        if land_use_freq > 0:
            print_str = (
                "Simulating land use development from {0} "
                "to {1} with {2}.".format(year, forecast_year, land_use_image))
            formatted_print(print_str)

            # 1. RUN URBANSIM
            formattable_usim_cmd = (
                '-y {0} -o {1} -v {2} -b {3} --scenario {4} -u {5} -w')
            usim_cmd = formattable_usim_cmd.format(
                year, forecast_year, land_use_freq, usim_bucket,
                scenario, path_to_beam_skims)
            usim = client.containers.run(
                land_use_image,
                command=usim_cmd, stdout=docker_stdout,
                stderr=True, detach=True, remove=True)
            for log in usim.logs(
                    stream=True, stderr=True, stdout=docker_stdout):
                print(log)

            # 2. COPY URBANSIM OUTPUT --> ACTIVITYSIM INPUT
            usim_data_path = formattable_s3_path.format(
                bucket=usim_bucket, io='output', scenario=scenario,
                year=forecast_year, fname='model_data.h5')
            asim_data_path = formattable_s3_path.format(
                bucket=asim_bucket, io='input', scenario=scenario,
                year=forecast_year, fname='model_data.h5')

            if not s3.exists(usim_data_path):
                raise FileNotFoundError(
                    "{0} failed to generate output data.".format(
                        land_use_image))
            else:
                print_str = (
                    'Copying data from {0} to {1} input directory').format(
                    usim_data_path, asim_data_path)
                formatted_print(print_str)
                s3.cp(usim_data_path, asim_data_path)

        # 3. PREPROCESS DATA FOR ACTIVITYSIM
        asim_data_dir = os.path.join('pilates', 'activitysim', 'data')

        # parse skims
        if not os.path.exists(os.path.join(asim_data_dir, "skims.omx")):
            create_skims_from_beam(asim_data_dir, configs)

        # # 3. RUN ACTIVITYSIM
        # print_str = (
        #     "Generating activity plans for the year "
        #     "{0} with {1}".format(
        #         forecast_year, activity_demand_image))
        # formatted_print(print_str)
        # formattable_asim_cmd = '-y {0} -s {1} -b {2} -u {3} -h {4} -w'
        # asim = client.containers.run(
        #     activity_demand_image, working_dir=asim_workdir,
        #     command=formattable_asim_cmd.format(
        #         forecast_year, scenario, asim_bucket, path_to_beam_skims,
        #         household_sample_size),
        #     stdout=docker_stdout, stderr=True, detach=True, remove=True)
        # for log in asim.logs(stream=True, stderr=True, stdout=docker_stdout):
        #     print(log)

        # asim_beam_data_path = formattable_s3_path.format(
        #     bucket=asim_bucket, io='output', scenario=scenario,
        #     year=forecast_year, fname='asim_outputs.zip')
        # if not s3.exists(asim_data_path):
        #     raise FileNotFoundError(
        #         "{0} failed to generate output data for BEAM.".format(
        #             activity_demand_image))

        # asim_usim_data_path = formattable_s3_path.format(
        #     bucket=usim_bucket, io='input', scenario=scenario,
        #     year=forecast_year, fname='model_data.h5')
        # if not s3.exists(asim_data_path):
        #     raise FileNotFoundError(
        #         "{0} failed to generate output data for BEAM.".format(
        #             activity_demand_image))

        # # 4. COPY ACTIVITYSIM OUTPUT --> URBANSIM INPUT

        # # If generating activities for the base year, don't overwrite
        # # urbansim input data. This is usually only the case for warm
        # # starts or debugging. Otherwise we want to set up urbansim for
        # # the next simulation iteration
        # if forecast_year != start_year:
        #     usim_data_path = formattable_s3_path.format(
        #         bucket=usim_bucket, io='input', scenario=scenario,
        #         year=forecast_year, fname='model_data.h5')
        #     asim_data_path = formattable_s3_path.format(
        #         bucket=asim_bucket, io='output', scenario=scenario,
        #         year=forecast_year, fname='model_data.h5')

        #     if not s3.exists(asim_data_path):
        #         raise FileNotFoundError(
        #             "{0} failed to generate output data.".format(
        #                 activity_demand_image))
        #     else:
        #         print_str = (
        #             'Copying data from {0} to {1} '
        #             'input directory'.format(
        #                 asim_data_path, usim_data_path))
        #         formatted_print(print_str)
        #         s3.cp(asim_data_path, usim_data_path)

        # # run beam
        # if not os.path.exists(beam_local_output_folder_name):
        #     os.mkdir(beam_local_output_folder_name)

        # path_to_beam_config = os.path.join(
        #     beam_local_input_folder_name, "input", beam_subdir,
        #     beam_local_config)
        # client.containers.run(
        #     travel_model_image,
        #     volumes={
        #         beam_local_input_folder_name: {
        #             'bind': '/app/{0}'.format(beam_local_input_folder_name),
        #             'mode': 'rw'},
        #         beam_local_output_folder_name: {
        #             'bind': '/app/output',
        #             'mode': 'rw'}},
        #     command="--config={0}".format(path_to_beam_config),
        #     stdout=docker_stdout, stderr=True, detach=True, remove=True
        # )

        # # copy beam skims to ???
