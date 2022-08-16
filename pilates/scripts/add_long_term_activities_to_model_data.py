import yaml
import os

if __name__ == '__main__':

    os.chdir('../..')
    from run import warm_start_activities, initialize_docker_client

    with open('settings.yaml') as f:
        settings = yaml.load(f, Loader=yaml.FullLoader)
    settings.update({'docker_stdout': True})
    year = settings['start_year']

    client = initialize_docker_client(settings)

    warm_start_activities(settings, year, client)
