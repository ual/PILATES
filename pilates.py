import yaml
import docker
import os
import s3fs

region_to_asim_subdir = {
    'austin': 'austin_mp',
    'detroit': 'detroit',
    'sfbay': 'bay_area_mp'
}

region_to_usim_bucket = {
    'austin': 'austin-urbansim',
    'detroit': 'detroit-urbansim',
    'sfbay': 'bayarea-urbansim'
}

region_to_asim_bucket = {
    'austin': 'austin-activitysim',
    'detroit': 'detroit-activitysim',
    'sfbay': 'bayarea-activitysim'
}

# TO DO: get usim and asim to read skims from bucket
# with prefixes instead of URL
region_to_skims_bucket = {
    'austin': 'austin-skims',
    'detroit': 'detroit-skims',
    'sfbay': 'bayarea-skims'
}

if __name__ == '__main__':

    client = docker.from_env()

    # read config
    with open('settings.yaml') as file:
        configs = yaml.load(file, Loader=yaml.FullLoader)

    land_use_image = configs['land_use_image']
    activity_demand_image = configs['activity_demand_image']
    travel_model_image = configs['travel_model_image']

    region = configs['region']
    scenario = configs['scenario']

    usim_bucket = region_to_usim_bucket[region]
    asim_bucket = region_to_asim_bucket[region]
    asim_subdir = region_to_asim_subdir[region]
    asim_workdir = os.path.join('/activitysim', asim_subdir)

    start_year = configs['start_year']
    end_year = configs['end_year']
    land_use_freq = configs['land_use_freq']
    travel_model_freq = configs['travel_model_freq']
    beam_skims_url = configs['beam_skims_url']

    # make sure user that executes pilates.py can run docker without sudo
    client = docker.from_env()
    # for year in range(start_year, end_year, travel_model_freq):
    for year in [start_year]:

        forecast_year = year + travel_model_freq
        print(
            "Simulating land use development from {0} to {1} with {2}.".format(
                year, forecast_year, land_use_image))

        # run urbansim
        # TO DO: FIX STDOUT PRINTING SO IT DOESN'T LOOK LIKE GARBAGE
        # TO DO: FIX PYPROJ ERRORS (prob use "conda run -n" as entrypoint)

        usim = client.containers.run(
            land_use_image,
            command="-i {0} -o {1} -v {2} -b {3} --scenario {4} -u {5}".format(
                year, forecast_year,
                land_use_freq, usim_bucket, scenario, beam_skims_url),
            stderr=True, detach=True, remove=True)
        for log in usim.logs(stream=True, stderr=True, stdout=True):
            print(log)

        # copy urbansim outputs to activitysim inputs
        formattable_data_path = os.path.join(
            '{0}', '{1}', '{2}', '{3}', 'model_data.h5')
        usim_data_path = formattable_data_path.format(
            usim_bucket, 'output', scenario, forecast_year)
        asim_data_path = formattable_data_path.format(
            asim_bucket, 'input', scenario, forecast_year)

        print('Copying data from {0} to {1} input directory'.format(
            asim_data_path, usim_data_path))

        s3fs.S3FileSystem.read_timeout = 84600
        s3 = s3fs.S3FileSystem(config_kwargs={'read_timeout': 86400})
        s3.cp(usim_data_path, asim_data_path)

        # run activitysim
        print("Generating activity plans for the year {0} with {1}".format(
            forecast_year, activity_demand_image))

        # only copy results to urbansim inputs if not base year. usually
        # only the case for warm start or debugging.
        if forecast_year == start_year:
            formattable_asim_cmd_str = '-y {0} -s {1} -b {2} -u {3}'
        else:
            formattable_asim_cmd_str = '-y {0} -s {1} -b {2} -u {3} -w'

        asim = client.containers.run(
            activity_demand_image, working_dir=asim_workdir,
            command=formattable_asim_cmd_str.format(
                forecast_year, scenario, asim_bucket, beam_skims_url),
            stdout=True, stderr=True, detach=True, remove=True)
        for log in asim.logs(stream=True, stderr=True, stdout=True):
            print(log)

        break

        # # copy activitysim outputs to beam inputs

        # # run beam
        # client.containers.run(
        #     travel_model_image,
        #     "-y {0}".format(sim_year))

        # # copy beam skims to urbansim inputs
