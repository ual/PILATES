import yaml
import docker


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

    start_year = configs['start_year']
    end_year = configs['end_year']
    land_use_freq = configs['land_use_freq']
    activity_demand_freq = configs['activity_demand_freq']
    travel_model_freq = configs['travel_model_freq']
    beam_skims_url = configs['beam_skims_url']

    for year in range(start_year, end_year, travel_model_freq):

        sim_year = year
        print(sim_year, sim_year + travel_model_freq)

        # copy base year skims to urbansim data bucket

        # run urbansim
        client.containers.run(
            land_use_image,
            "-i {0} -o {1} -v {2} -b {3} --scenario {4} -u {5}".format(
                sim_year, sim_year + travel_model_freq,
                land_use_freq, usim_bucket, scenario, beam_skims_url))

    #     # copy urbansim outputs to activitysim inputs

    #     # run activitysim
    #     sim_year = sim_year + travel_model_freq
    #     client.containers.run(
    #         activity_demand_image,
    #         command='-y {0} -s {1} -b {2} -w'.format(
    #             sim_year, scenario, asim_bucket))

    #     # copy activitysim outputs to beam inputs

    #     # run beam
    #     client.containers.run(
    #         travel_model_image,
    #         "-y {0}".format(sim_year))

    #     # copy beam skims to urbansim inputs
