import os
import pandas as pd
import numpy as np
import logging
from scipy import signal
from matplotlib import pyplot as plt
from pdb import set_trace as st
from scipy.interpolate import interp1d
import random
import geopandas as gpd
import json
import matplotlib.path as mpltPath
from tqdm import tqdm
import time
#from activitysim.core import inject
DEBUG = False


# ============================================================================================
def upsample_departures_to_minutes(df_car_trips,
        interpolation_type = 'cubic',
        plot = False):
    assert interpolation_type == 'cubic' or interpolation_type == 'linear', "Invalid interpolation type."

    departure_times_upsampling_multiplier = 60

    # Interpolate departure times
    departures_and_hours = df_car_trips['depart'].value_counts().sort_index()
    original_hours = departures_and_hours.index.values
    hours = [int(h * departure_times_upsampling_multiplier) for h in original_hours]

    initial_hour = int(min(hours) - departure_times_upsampling_multiplier)
    finish_hour = int(max(hours) + departure_times_upsampling_multiplier)
    print("Adding a zero starting and ending point to the departure times distribution. ")
    print("Starting at {} and ending at {}.".format(initial_hour, finish_hour))
    hours = np.array([initial_hour] + hours + [finish_hour])
    departures = np.concatenate(([0], departures_and_hours.values, [0]))

    hours_upsampled = np.linspace(hours.min(), hours.max()+1, (hours.max()+1 - hours.min()), endpoint=False)
    interpolation = interp1d(hours, departures, kind=interpolation_type)

    if plot:
        plt.plot(hours, departures, 'o', hours_upsampled, interpolation(hours_upsampled), '.-')
        plt.savefig('departures_upsampled_to_minutes_interpolation_{}.png'.format(interpolation_type))
        plt.clf()

    departures_reassigned = []
    for hour in hours[1:-1]:
        time_range_start = int(hour - departure_times_upsampling_multiplier / 2)
        time_range_end = int(hour + departure_times_upsampling_multiplier / 2)
        departures_in_time_range = len(df_car_trips[df_car_trips['depart'] == int(hour / departure_times_upsampling_multiplier)])
        time_range = np.linspace(time_range_start, time_range_end, time_range_end - time_range_start, endpoint = False)
        interpolated_weights = interpolation(hours_upsampled)[time_range_start-hours[0]:time_range_end-hours[0]]
        departures_reassigned += random.choices (time_range, \
                            weights=interpolated_weights, \
                            k = departures_in_time_range)
    
    if plot:
        plt.xlim([min(departures_reassigned)-5, max(departures_reassigned)+5])
        plt.hist(departures_reassigned, bins = hours_upsampled)
        plt.savefig('departures_upsampled_to_minutes_histogram_{}.png'.format(interpolation_type))
        plt.clf()

    return (hours_upsampled, departures_reassigned)

# ============================================================================================
"""
    1. Filters the trips to only car trips.
    2. Upsamples the departure times from hours into minutes using upsample_departures_to_minutes
    3. Upsamples the departure times from minutes into seconds using an uniform distribution

    Returns the car trips of the original DataFrame, with an additional column "dep_time", upsampled to seconds
"""
def upsample_departures_to_seconds(df_car_trips, interpolation_type, plot = False):
    assert ActivitySim_output_tables_path[-3:] == '.h5'
    assert interpolation_type == 'cubic' or interpolation_type == 'linear'

    hours_upsampled, departures_reassigned = upsample_departures_to_minutes(
                df_car_trips,
                interpolation_type = interpolation_type)

    df_departures_reassigned_with_seconds = [int(dep*60 + random.randint(0, 59)) for dep in departures_reassigned]

    if plot:
        plt.hist(df_departures_reassigned_with_seconds, bins = hours_upsampled)
        plt.savefig('departures_upsampled_to_seconds_sampling_{}.png'.format(interpolation_type))
        plt.clf()

    return df_departures_reassigned_with_seconds

"""
    Rounds a list of departures in seconds to the hour.
    i.e
    13:01:00 -> 13
    13:29:59 -> 13
    13:30:00 -> 14
"""
def downsample_upsampled_departures(df_departures_reassigned_with_seconds):
    departures_reassigned_rounded = [round(d/3600 + 0.0001) for d in df_departures_reassigned_with_seconds]
    plt.hist(departures_reassigned_rounded, bins = 19)
    plt.savefig('departures_upsampled_downsampled.png')
    plt.clf()
    return departures_reassigned_rounded

"""
    Receives a row of ActivitySim's TAZ's table and returns all nodes inside the TAZ's geometry
"""
def generate_random_points_within_TAZ(row, points, df_nodes):
    ### return the indices of points in df_nodes that are contained in row['geometry']
    TAZs_with_no_nodes_inside = []

    if row['geometry'].type == 'MultiPolygon':
        coords = [list(x.exterior.coords) for x in row['geometry'].geoms]
        flat_coords = [coord for sublist_coords in coords for coord in sublist_coords]
        path = mpltPath.Path(flat_coords)
    else:
        path = mpltPath.Path(list(zip(*row['geometry'].exterior.coords.xy)))

    in_index = path.contains_points(points)
    nodes_inside_the_TAZ = df_nodes['index'].loc[in_index].tolist()

    return df_nodes['index'].loc[in_index].tolist()

"""
    Given a dataframe of trips, adds a column "in_nodes" with all the nodes from the given nodes_filepath
    which are inside each row's TAZ Polygon or MultiPolygon.
"""
def assign_random_points_within_TAZs(df_trips, zones_filepath, nodes_filepath):
    print("Generating random points...")
    assert zones_filepath[-3:] == 'shp', "Zones filepath should be in SHP format"
    assert nodes_filepath[-3:] == 'csv', "Nodes filepath should be in CSV format"

    df_nodes = pd.read_csv(nodes_filepath)
    zones = gpd.read_file(zones_filepath)

    zones = zones.to_crs({'init':'epsg:4326'}) # it should already be in EPSG:4326 but we cast it just in case
    points = df_nodes[['x', 'y']].values # x, y are the coordinates of the nodes
    zones['in_nodes'] = zones.apply(lambda row: generate_random_points_within_TAZ(row, points, df_nodes), axis=1)

    print("Checking that zones with no intersections inside have no trips starting or ending there...")
    df_zones_with_no_intersections_inside = zones[zones['in_nodes'].str.len() == 0]
    print("Zones with no intersections inside: \n {}".format(df_zones_with_no_intersections_inside))
    for TAZ_id, row in df_zones_with_no_intersections_inside.iterrows():
        print("The TAZ {} has no nodes inside. Checking if there are any trips that start or end there...".format(TAZ_id))
        trips_that_depart_from_empty_TAZ = df_trips[df_trips['origin'] == TAZ_id]
        assert len(trips_that_depart_from_empty_TAZ) == 0, "No trips should depart from a TAZ with no intersections."
        trips_that_go_to_empty_TAZ = df_trips[df_trips['destination'] == TAZ_id]
        assert len(trips_that_go_to_empty_TAZ) == 0, "No trips should end in a TAZ with no intersections."
        print("Passed.")
        
    print('Assigning random points...')
    
    start = time.time()

    # Some people may depart from TAZs that have no nodes inside
    # An example in the bay area is the TAZ 931, which includes only the San Quentin State Prison
    people_that_depart_from_TAZs_without_nodes_inside = []
    people_that_go_to_TAZs_without_nodes_inside = []
    origin_TAZs_with_no_nodes_inside = []
    dest_TAZs_with_no_nodes_inside = []

    iterate_one_by_one = False
    if iterate_one_by_one:
        for i, row in tqdm(enumerate(df_trips.itertuples(), 0), total = len(df_trips)):
            taz_origin_rows = zones[zones['taz1454'] == row.origin]
            taz_destination_rows = zones[zones['taz1454'] == row.destination]

            if len(taz_origin_rows['in_nodes'].iloc[0]) == 0:
                people_that_depart_from_TAZs_without_nodes_inside.append(i)
                origin_TAZs_with_no_nodes_inside.append(row.origin)
            elif len(taz_destination_rows['in_nodes'].iloc[0]) == 0:
                people_that_go_to_TAZs_without_nodes_inside.append(i)
                dest_TAZs_with_no_nodes_inside.append(row.destination)
            else:
                try:
                    node_origin = random.choice(taz_origin_rows['in_nodes'].iloc[0])
                    node_destination = random.choice(taz_destination_rows['in_nodes'].iloc[0])
                    df_trips['origin'] = node_origin
                    df_trips['destination'] = node_destination
                except Exception as exc:
                    print("Error! {}".format(exc))
                    st()
    else:
        df_car_trips_merged_with_TAZs_only_origins = pd.merge(df_trips, zones, left_on='origin', right_on='TAZ')
        df_car_trips_merged_with_TAZs = pd.merge(df_car_trips_merged_with_TAZs_only_origins, zones, left_on='destination', \
                                                right_on='TAZ', suffixes=('_origin', '_destination'))
        
        # We obtain a random node index inside the origin and destination TAZs
        df_car_trips_merged_with_TAZs['destination_index'] = df_car_trips_merged_with_TAZs['in_nodes_destination'].\
            apply(lambda node_list: random.choice(node_list))
        df_car_trips_merged_with_TAZs['origin_index'] = df_car_trips_merged_with_TAZs['in_nodes_origin'].\
            apply(lambda node_list: random.choice(node_list))
        
        # We merge again to obtain the origin and destination osmid corresponding to that index
        df_nodes_osmid_and_index = df_nodes[['osmid', 'index']]
        df_car_trips_merged_with_TAZs = pd.merge(df_car_trips_merged_with_TAZs, df_nodes_osmid_and_index, \
            left_on='origin_index', right_on='index')
        df_car_trips_merged_with_TAZs = pd.merge(df_car_trips_merged_with_TAZs, df_nodes_osmid_and_index, \
            left_on='destination_index', right_on='index', suffixes=('_origin', '_destination'))

    if len(people_that_depart_from_TAZs_without_nodes_inside) > 0:
        print("Warning! {} people depart from TAZs that have no nodes inside: {}"\
            .format(len(people_that_depart_from_TAZs_without_nodes_inside), \
            people_that_depart_from_TAZs_without_nodes_inside))        
        print("Origin TAZs with no nodes inside: {}".format(origin_TAZs_with_no_nodes_inside))
    if len(people_that_go_to_TAZs_without_nodes_inside) > 0:
        print("Warning! {} people go to TAZs that have no nodes inside: {}"\
            .format(len(people_that_go_to_TAZs_without_nodes_inside), \
            people_that_go_to_TAZs_without_nodes_inside))        
        print("Destination TAZs with no nodes inside: {}".format(dest_TAZs_with_no_nodes_inside))

    print("Total time for assigning random points {} secs".format(time.time() - start))


    return df_car_trips_merged_with_TAZs




"""
    Plots a histogram of the original departure times for ActivitySim's car trips
"""
def original_departures(ActivitySim_output_tables_path):
    car_trips = load_car_trips(ActivitySim_output_tables_path)
    print("{} car_trips found".format(len(car_trips)))
    if plot:
        plt.hist(car_trips['depart'], bins = 19)
        plt.savefig('original_departures.png')
        plt.clf()

    return car_trips


"""
    Reads ActivitySim's output and returns a DataFrame with only the car trips
"""
def load_car_trips(ActivitySim_output_tables_path):
    print("Loading car trips...")
    hdf_activitySim_output = pd.HDFStore(ActivitySim_output_tables_path, mode = 'r')
    all_trips = hdf_activitySim_output['/trips']
    st()
    # Filter trip modes
    car_trips_names = ['DRIVEALONEFREE', 'SHARED2FREE', 'SHARED3FREE', 
                    'DRIVE_HVY', 'DRIVE_LRF','DRIVE_LOC', 'DRIVEALONEPAY', 
                    'DRIVE_EXP', 'SHARED2PAY', 'DRIVE_COM']

    car_trips = all_trips[all_trips.trip_mode.isin(car_trips_names)].reset_index()
    print("{} car trips found".format(len(car_trips)))
    car_trips = car_trips.sort_values(by=['depart', 'person_id','trip_count'])
    return car_trips

"""
    1. Filters the trips and keeps only car trips
    2. Upsamples their departure times in hours, adding minutes and seconds
    3. Assigns a random intersection inside the TAZs of origin and destination of each trip
    Saves the resulting DataFrame as a csv
"""
def translate_activitySim_output_to_MANTA_demands(ActivitySim_output_tables_path, nodes_filepath,
    zones_filepath, output_filepath, departure_times_interpolation_type = 'cubic'):

    assert output_filepath[-4:] == '.csv'
    time_start = time.time()
    df_car_trips = load_car_trips(ActivitySim_output_tables_path)
    time_after_loading = time.time()
    print("Loading time: {}".format(round(time_after_loading - time_start)),2)

    #FIXME parallelize the following two steps with two threads for better performance
    # We assign random points within the TAZs' polygons
    df_car_trips = assign_random_points_within_TAZs(df_car_trips, zones_filepath, nodes_filepath)
    time_after_assigning_random_points = time.time()
    print("Assigning points within TAZs time: {}".format(round(time_after_assigning_random_points - time_after_loading)),2)

    # We upsample the departure times from hours to seconds
    df_car_trips['dep_time'] = upsample_departures_to_seconds(df_car_trips, departure_times_interpolation_type)
    time_after_upsampling_departures_to_seconds = time.time()
    print("Upsampling departures to seconds: {}".format(round(time_after_upsampling_departures_to_seconds - time_after_assigning_random_points)),2)

    # FIXME: PERNO is a parameter that currently MANTA expects, but does not do anything.
    # We should either implement its functionality or delete it
    df_car_trips['PERNO'] = 1

    df_car_trips = df_car_trips[['person_id', 'PERNO', 'osmid_origin', 'osmid_destination', 'dep_time', 'index_origin', 'index_destination']]
    df_car_trips.columns = ['SAMPN', 'PERNO', 'origin_osmid', 'destination_osmid', 'dep_time', 'origin', 'destination']
    df_car_trips.to_csv(output_filepath)

    st()
    return df_car_trips


if __name__ == "__main__":
    ActivitySim_output_tables_path = 'output/final_output_tables.h5'
    nodes_filepath = 'nodes.csv'
    zones_filepath = 'bay_area_TAZs.shp'
    output_filepath = 'od_demand_5to24.csv'
    
    translate_activitySim_output_to_MANTA_demands(ActivitySim_output_tables_path, nodes_filepath, zones_filepath, output_filepath)



# ===========================================================================================================================
# ========================================================== Tests ==========================================================
# ===========================================================================================================================
"""
    The following test takes an upsampled list of departures and rounds their departure times. Then asserts that
    all rounded departure times are the same as their original departure times.

    For instance, in the following example Person 0 would pass the test but Person 1 would not:
    Person      upsampled departure time        original departure time
    0                13:23:05                             13
    1                20:15:06                             19

"""
def test_departure_upsampling_consistency(df_car_trips, departures_reassigned_with_seconds):
    car_trips_ordered_by_depart = df_car_trips.sort_values(by=['depart', 'person_id','trip_count'])
    departures_reassigned_rounded = downsample_upsampled_departures(departures_reassigned_with_seconds)
    car_trips_ordered_by_depart['dep_time_rounded'] = pd.DataFrame(departures_reassigned_rounded).values
    car_trips_ordered_by_depart['dep_time'] = pd.DataFrame(departures_reassigned_with_seconds).values
    assert len(car_trips_ordered_by_depart[car_trips_ordered_by_depart['depart'].astype('int') != car_trips_ordered_by_depart['dep_time_rounded']]) == 0, \
        'All departure times should be the same as the original when rounded.'

"""
    This test runs the test_departure_upsampling_consistency test for both linear and cubic interpolations
"""
def test_departure_times_sampling(ActivitySim_output_tables_path):
    print("Plotting original departures...")
    df_car_trips = original_departures(ActivitySim_output_tables_path)

    print("Testing linear interpolation...")
    #FIXME: Test this test
    departures_reassigned_with_seconds_linear = upsample_departures_to_seconds(df_car_trips, 'linear')
    test_departure_upsampling_consistency(df_car_trips, departures_reassigned_with_seconds_linear)

    print("Testing cubic interpolation...")
    departures_reassigned_with_seconds_cubic = upsample_departures_to_seconds(df_car_trips, 'cubic')
    test_departure_upsampling_consistency(df_car_trips, departures_reassigned_with_seconds_cubic)

"""
    Converts and saves a geopandas variable into a geojson file
"""
def save_geopandas_as_geojson(gdp_input, filename):
    geojson = json.loads(gdp_input.to_json())
    with open(filename, 'w') as outfile:
        json.dump(geojson, outfile)


"""
    This test saves a geojson file with all the TAZs that are not the origin for any trips
    and those which are not the destination for any trips.
    The recommendation is to plot this geojson with kepler.gl to visualize the results.
"""
def test_TAZ_presence():

    print("Testing TAZ presence...")
    all_taz = gpd.read_file("bay_area_TAZs.shp")
    # It should be in EPSG:4326 but we cast it just in case
    try:
        all_taz = all_taz.to_crs('epsg:4326')
    except:
        print("Changing the way CRS is set because of the Python version...")
        all_taz = all_taz.to_crs({'init':'epsg:4326'})

    car_trips = load_car_trips(ActivitySim_output_tables_path = ActivitySim_output_tables_path)
    no_origin_TAZ = all_taz[~all_taz.TAZ.isin(car_trips.origin)]
    no_origin_TAZ_ids = no_origin_TAZ['TAZ'].unique()

    if len(no_origin_TAZ_ids) == 0:
        print("No TAZs were found with no origin") 
    else:
        print("{} TAZs were found with no origin: {}".format(len(no_origin_TAZ_ids), no_origin_TAZ_ids)) 
    save_geopandas_as_geojson(no_origin_TAZ, 'no_origin_TAZ.json')