import gzip
import os
import pandas as pd
import numpy as np
import geopandas as gpd
import h5py
import glob
from zipfile import ZipFile
import shutil
import os
from pilates.utils.geog import get_taz_geoms
from pilates.utils.geog import get_taz_labels
from pilates.utils.io import parse_args_and_settings
from joblib import Parallel, delayed
from multiprocessing import cpu_count
import zipfile
from datetime import date
import logging
from pilates.activitysim.postprocessor import get_usim_datastore_fname

logger = logging.getLogger(__name__)

dtypes = {
    "time": "float32",
    "type": "category",
    "legMode": "category",
    "actType": "category",
    "primaryFuelLevel": "float64",
    "chargingPointType": "category",
    "pricingModel": "category",
    "parkingType": "category",
    "mode": "category",
    "personalVehicleAvailable": "category",
    "person": "str",
    "driver": "str",
    "riders": "str",
    'primaryFuelType': "category",
    'secondaryFuelType': 'category',
    'currentTourMode': 'category',
    'currentActivity': 'category',
    'nextActivity': 'category',
    'tripId': 'Int64'
}


def copy_outputs_to_mep(settings, year, iter):
    asim_output_data_dir = settings['asim_local_output_folder']
    mep_output_data_dir = os.path.join(settings['mep_local_output_folder'], str(year))
    if not os.path.exists(mep_output_data_dir):
        os.makedirs(mep_output_data_dir)
    beam_iter_output_dir = os.path.join(settings['beam_local_output_folder'], settings['region'],
                                        "year-{0}-iteration-{1}".format(year, iter))

    def copy_with_compression_asim_file_to_mep(asim_file_name, mep_file_name):
        asim_file_path = os.path.join(asim_output_data_dir, asim_file_name)
        mep_file_path = os.path.join(mep_output_data_dir, mep_file_name)
        logger.info("Copying asim file %s to beam input scenario file %s", asim_file_path, mep_file_path)

        if os.path.exists(asim_file_path):
            with open(asim_file_path, 'rb') as f_in, gzip.open(
                    mep_file_path, 'wb') as f_out:
                f_out.writelines(f_in)

    def copy_urbansim_outputs_to_mep():
        data_dir = settings['usim_local_data_folder']
        usim_output_store_name = get_usim_datastore_fname(
            settings, io='input', year=year)
        usim_output_store_path = os.path.join(data_dir, usim_output_store_name)
        if not os.path.exists(usim_output_store_path):
            raise ValueError('No output data store found at {0}'.format(
                usim_output_store_path))
        usim_output_store = pd.HDFStore(usim_output_store_path)
        jobs = usim_output_store.get('jobs')
        blocks = usim_output_store.get('blocks')
        jobs.to_csv(os.path.join(mep_output_data_dir, "jobs.csv.gz"))
        blocks.to_csv(os.path.join(mep_output_data_dir, "blocks.csv.gz"))

    def copy_asim_files_to_mep():
        ds = pd.HDFStore(os.path.join(asim_output_data_dir, "pipeline.h5"))
        trips = ds.get('trips/trip_mode_choice')
        trips.value_counts(["destination", "purpose"]).unstack(fill_value=0).to_csv(
            os.path.join(mep_output_data_dir, "activity_frequency.csv.gz"))
        copy_with_compression_asim_file_to_mep('final_plans.csv', 'plans.csv.gz')
        copy_with_compression_asim_file_to_mep('final_households.csv', 'households.csv.gz')
        copy_with_compression_asim_file_to_mep('final_persons.csv', 'persons.csv.gz')
        copy_with_compression_asim_file_to_mep('final_land_use.csv', 'land_use.csv.gz')

    def copy_beam_files_to_mep():
        shutil.copy(os.path.join(beam_iter_output_dir, "network.csv.gz"),
                    os.path.join(mep_output_data_dir, "network.csv.gz"))
        linkstats_path = os.path.join(beam_iter_output_dir, "ITERS", "it.0", "0.linkstats.csv.gz")
        try:
            shutil.copy(linkstats_path, os.path.join(mep_output_data_dir, "linkstats.csv.gz"))
        except:
            logger.error("Missing expected beam output file {0}".format(linkstats_path))
        parkingStats = os.path.join(beam_iter_output_dir, "ITERS", "it.0", "0.parkingStats.csv")
        try:
            shutil.copy(parkingStats, os.path.join(mep_output_data_dir, "parkingStats.csv"))
        except:
            logger.error("Missing expected beam output file {0}".format(parkingStats))
        ridehailSkims = os.path.join(beam_iter_output_dir, "ITERS", "it.0", "0.skimsRidehail.csv.gz")
        try:
            shutil.copy(ridehailSkims, os.path.join(mep_output_data_dir, "ridehailSkims.csv.gz"))
        except:
            logger.error("Missing expected beam output file {0}".format(ridehailSkims))
        odSkims = os.path.join(beam_iter_output_dir, "ITERS", "it.0", "0.skimsTAZ.csv.gz")
        try:
            shutil.copy(odSkims, os.path.join(mep_output_data_dir, "odSkims.csv.gz"))
        except:
            logger.error("Missing expected beam output file {0}".format(odSkims))
        beam_router_dir = os.path.join(settings['beam_local_input_folder'], settings['region'],
                                       settings['beam_router_directory'])
        mep_gtfs_dir = os.path.join(mep_output_data_dir, "GTFS")
        if not os.path.exists(mep_gtfs_dir):
            os.makedirs(mep_gtfs_dir)
        for file in os.listdir(beam_router_dir):
            if file.endswith(".zip"):
                shutil.copy(os.path.join(beam_router_dir, file), os.path.join(mep_gtfs_dir, file))
        try:
            shutil.copy(os.path.join(beam_iter_output_dir, "totalsByMode.csv"),
                        os.path.join(mep_gtfs_dir, "totalsByMode.csv"))
        except:
            logger.error("Totals by mode were not generated by the postprocessor, expected at {0}".format(
                os.path.join(beam_iter_output_dir, "totalsByMode.csv")))

        # Also add skims, ridehail and parking info

    copy_urbansim_outputs_to_mep()
    copy_asim_files_to_mep()
    copy_beam_files_to_mep()


def _load_events_file(settings, year, replanning_iteration_number, beam_iteration=0):
    beam_output_dir = settings['beam_local_output_folder']
    region = settings['region']
    iteration_output_dir = "year-{0}-iteration-{1}".format(year, replanning_iteration_number)
    events_dir = os.path.join("ITERS", "it.{0}".format(beam_iteration), "{0}.events.csv.gz".format(beam_iteration))
    path = os.path.join(beam_output_dir, region, iteration_output_dir, events_dir)
    events = pd.read_csv(path, dtype=dtypes)

    # Adding scenario info
    scenario_defs = settings['scenario_definitions']
    events['scenario'] = scenario_defs['name']
    events['scenario'] = events['scenario'].astype("category")
    events['lever'] = scenario_defs['lever']
    events['lever'] = events['lever'].astype("category")
    events['year'] = year
    events['lever_position'] = scenario_defs['lever_position']

    return events


def _reformat_events_file(events):
    # Rename the "mode" column

    events.rename(columns={"mode": "modeBEAM"}, inplace=True)

    # Replace "Work" with "work" in the "actType" column
    events["actType"].replace({"Work": "work"}, inplace=True)

    initialParkingEvents = (events["type"] == "ParkingEvent") & (events["time"] == 0)
    events = events[~events.person.str.contains("Agent", na=False) & ~initialParkingEvents].reset_index(drop=True)


    # shift column 'person' to first position
    first_column = events.pop('person')
    second_column = events.pop('driver')
    third_column = events.pop('riders')

    # insert column using insert(position,column_name,first_column) function
    events.insert(0, 'person', first_column)
    events.insert(1, 'driver', second_column)
    events.insert(2, 'riders', third_column)

    # Adding the IDMerged Column
    events['UniqueID'] = events['person']  # make a copy of the person column
    events['personID'] = np.where(events['person'].isin(events['driver']), events['person'], np.nan)
    events['driverID'] = np.where(events['driver'].isin(events['person']), events['driver'], np.nan)

    # Merging person and driver ids in one column
    events['IDMerged'] = events['personID'].combine_first(events['driverID'])
    events['IDMerged'] = events['UniqueID'].combine_first(events['IDMerged'])

    # Dropping unused columns
    events = events.drop(['personID', 'driverID', 'UniqueID'], axis=1)

    # Shift column 'IDMerged' to first position
    first_column = events.pop('IDMerged')
    # Insert column using insert(position,column_name,first_column) function
    events.insert(0, 'IDMerged', first_column)

    # Split the "riders' column and replicated rows for every rider
    events['riders'] = events['riders'].str.split(':')
    events = events.explode('riders')

    # Combine riderID with IDMerged
    events['riderID'] = np.where(events['riders'].isin(events['person']), events['riders'], np.nan)
    events['IDMerged'] = events['riderID'].combine_first(events['IDMerged'])

    # Dropping unused columns
    events = events.drop(['riderID'], axis=1)

    # Remove driver = TransitDriver or RidehailDriver for IDMerged = NAN because there are no agent information in
    # these rows
    events = events[~((events.driver.str.contains("Agent", na=False)) & (events.IDMerged.isna()))].reset_index(
        drop=True)

    events["chargeID"] = events.groupby('vehicle')['IDMerged'].transform(lambda x: x.ffill().bfill())

    # Combining chargeID with IDMerged so no NANs anymore
    events['IDMerged'] = events['chargeID'].combine_first(events['IDMerged'])

    # Dropping unused columns
    events = events.drop(['chargeID'], axis=1)

    # Change the IDMerged column type to numeric
    events["IDMerged"] = pd.to_numeric(events.IDMerged)

    # Sort by IDMerged and time columns
    events = events.sort_values(['IDMerged', 'time']).reset_index(drop=True)

    # We assume that the number of passengers is 1 for ride_hail_pooled
    events['modeBEAM_rh'] = np.where(events.driver.str.contains("rideHailAgent", na=False), 'ride_hail',
                                     events['modeBEAM'])

    # Adding teleportation mode to the type = TeleportationEvent row
    events["modeBEAM_rh"] = np.where(events['type'] == 'TeleportationEvent',
                                     events.modeBEAM_rh.fillna(method='ffill'), events["modeBEAM_rh"])
    events['modeBEAM_rh_pooled'] = np.where(
        (events['type'] == 'PersonCost') & (events['modeBEAM'] == 'ride_hail_pooled'), 'ride_hail_pooled', np.nan)
    events['modeBEAM_rh_ride_hail_transit'] = np.where(
        (events['type'] == 'PersonCost') & (events['modeBEAM'] == 'ride_hail_transit'), 'ride_hail_transit', np.nan)
    events['modeBEAM_rh_pooled'] = events['modeBEAM_rh_pooled'].shift(+1)
    events['modeBEAM_rh_ride_hail_transit'] = events['modeBEAM_rh_ride_hail_transit'].shift(+1)
    events['modeBEAM_rh'] = np.where((events['type'] == 'PathTraversal') & (events['modeBEAM'] == 'car') & (
        events['driver'].str.contains("rideHailAgent", na=False)) & (events['modeBEAM_rh_pooled'] != 'nan'),
                                     events['modeBEAM_rh_pooled'], events['modeBEAM_rh'])
    # We don't know if ridehail_transit is ride_hail or ride_hail_pooled
    events['modeBEAM_rh'] = np.where((events['type'] == 'PathTraversal') & (events['modeBEAM'] == 'car') & (
        events['driver'].str.contains("rideHailAgent", na=False)) & (
                                             events['modeBEAM_rh_ride_hail_transit'] != 'nan'),
                                     events['modeBEAM_rh_ride_hail_transit'], events['modeBEAM_rh'])

    # Dropping the temporary columns
    events = events.drop(['modeBEAM_rh_pooled'], axis=1)
    events = events.drop(['modeBEAM_rh_ride_hail_transit'], axis=1)
    return events


def _expand_events_file(events):
    events['actEndTime'] = np.where(events['type'] == 'actend', events['time'], np.nan)
    events['actStartTime'] = np.where(events['type'] == 'actstart', events['time'], np.nan)
    events['duration_travelling'] = np.where(
        (events['type'] == 'PathTraversal') | (events['type'] == 'TeleportationEvent'),
        events['arrivalTime'] - events['departureTime'], np.nan)
    events['distance_travelling'] = np.where(
        (events['type'] == 'PathTraversal') |
        ((events['type'] == 'ModeChoice') & ((events['modeBEAM'] == 'hov2_teleportation') |
                                             (events['modeBEAM'] == 'hov3_teleportation'))),
        events['length'], np.nan)
    events['distance_mode_choice'] = np.where(events['type'] == 'ModeChoice', events['length'], np.nan)
    events['duration_walking'] = np.where(events['modeBEAM'] == 'walk', events['duration_travelling'], np.nan)
    events['distance_walking'] = np.where(events['modeBEAM'] == 'walk', events['distance_travelling'], np.nan)
    events['duration_on_bike'] = np.where(events['modeBEAM'] == 'bike', events['duration_travelling'], np.nan)
    events['distance_bike'] = np.where(events['modeBEAM'] == 'bike', events['distance_travelling'], np.nan)
    events['duration_in_ridehail'] = np.where(
        (events['modeBEAM_rh'] == 'ride_hail') |
        (events['modeBEAM_rh'] == 'ride_hail_pooled') |
        (events['modeBEAM_rh'] == 'ride_hail_transit'), events['duration_travelling'], np.nan)
    events['distance_ridehail'] = np.where(
        (events['modeBEAM_rh'] == 'ride_hail') |
        (events['modeBEAM_rh'] == 'ride_hail_pooled') |
        (events['modeBEAM_rh'] == 'ride_hail_transit'), events['distance_travelling'], np.nan)
    events['duration_in_privateCar'] = np.where(
        (events['modeBEAM_rh'] == 'car') |
        (events['modeBEAM_rh'] == 'car_hov3') |
        (events['modeBEAM_rh'] == 'car_hov2') |
        (events['modeBEAM_rh'] == 'hov2_teleportation') |
        (events['modeBEAM_rh'] == 'hov3_teleportation'), events['duration_travelling'], np.nan)
    events['distance_privateCar'] = np.where(
        (events['modeBEAM_rh'] == 'car') |
        (events['modeBEAM_rh'] == 'car_hov3') |
        (events['modeBEAM_rh'] == 'car_hov2') |
        (events['modeBEAM_rh'] == 'hov2_teleportation') |
        (events['modeBEAM_rh'] == 'hov3_teleportation'), events['distance_travelling'], np.nan)
    events['duration_in_transit'] = np.where(
        (events['modeBEAM'] == 'bike_transit') | (events['modeBEAM'] == 'drive_transit') |
        (events['modeBEAM'] == 'walk_transit') | (events['modeBEAM'] == 'bus') |
        (events['modeBEAM'] == 'tram') | (events['modeBEAM'] == 'subway') |
        (events['modeBEAM'] == 'rail') | (events['modeBEAM'] == 'cable_car') |
        (events['modeBEAM'] == 'ride_hail_transit'), events['duration_travelling'], np.nan)
    events['distance_transit'] = np.where(
        (events['modeBEAM'] == 'bike_transit') | (events['modeBEAM'] == 'drive_transit') |
        (events['modeBEAM'] == 'walk_transit') | (events['modeBEAM'] == 'bus') |
        (events['modeBEAM'] == 'tram') | (events['modeBEAM'] == 'subway') |
        (events['modeBEAM'] == 'rail') | (events['modeBEAM'] == 'cable_car') |
        (events['modeBEAM'] == 'ride_hail_transit'), events['distance_travelling'], np.nan)

    # Removing the extra tour index happening after replanning events
    events['replanningTime'] = np.where(events['type'] == 'Replanning', events['time'], np.nan)
    events['replanningTime'] = events['replanningTime'].shift(+1)
    events['tourIndex_fixed'] = np.where((events['type'] == 'ModeChoice') & (events['replanningTime'].notna()),
                                         np.nan, events['tourIndex'])
    events['fuelFood'] = np.where((events['type'] == 'PathTraversal') & (events['primaryFuelType'] == 'Food'),
                                  events['primaryFuel'], np.nan)
    events['emissionFood'] = events['fuelFood'] * 8.3141841e-9 * 0
    events['fuelElectricity'] = np.where(
        (events['type'] == 'PathTraversal') & (events['primaryFuelType'] == 'Electricity'),
        events['primaryFuel'], np.nan)
    events['emissionElectricity'] = events['fuelElectricity'] * 2.77778e-10 * 947.2 * 0.0005
    events['fuelDiesel'] = np.where((events['type'] == 'PathTraversal') & (events['primaryFuelType'] == 'Diesel'),
                                    events['primaryFuel'], np.nan)
    events['emissionDiesel'] = events['fuelDiesel'] * 8.3141841e-9 * 10.180e-3
    events['fuelBiodiesel'] = np.where(
        (events['type'] == 'PathTraversal') & (events['primaryFuelType'] == 'Biodiesel'),
        events['primaryFuel'], np.nan)
    events['emissionBiodiesel'] = events['fuelBiodiesel'] * 8.3141841e-9 * 10.180e-3
    events['fuel_not_Food'] = np.where((events['type'] == 'PathTraversal') & (events['primaryFuelType'] != 'Food')
                                       , events['primaryFuel'] + events['secondaryFuel'], np.nan)
    events['fuelGasoline'] = np.where((events['type'] == 'PathTraversal') & (
            (events['primaryFuelType'] == 'Gasoline') | (events['secondaryFuelType'] == 'Gasoline')),
                                      events['primaryFuel'] + events['secondaryFuel'], np.nan)
    events['emissionGasoline'] = events['fuelGasoline'] * 8.3141841e-9 * 8.89e-3

    # Marginal fuel
    conditions = [(events['modeBEAM_rh'] == 'ride_hail_pooled'),
                  (events['modeBEAM_rh'] == 'walk_transit') | (events['modeBEAM_rh'] == 'drive_transit') |
                  (events['modeBEAM_rh'] == 'ride_hail_transit') | (events['modeBEAM_rh'] == 'bus') | (
                          events['modeBEAM_rh'] == 'subway') |
                  (events['modeBEAM_rh'] == 'rail') | (events['modeBEAM_rh'] == 'tram') | (
                          events['modeBEAM_rh'] == 'cable_car') |
                  (events['modeBEAM_rh'] == 'bike_transit'),
                  (events['modeBEAM_rh'] == 'walk') | (events['modeBEAM_rh'] == 'bike'),
                  (events['modeBEAM_rh'] == 'ride_hail') | (events['modeBEAM_rh'] == 'car') |
                  (events['modeBEAM_rh'] == 'car_hov2') | (events['modeBEAM_rh'] == 'car_hov3') |
                  (events['modeBEAM_rh'] == 'hov2_teleportation') | (events['modeBEAM_rh'] == 'hov3_teleportation')]
    choices = [events['fuel_not_Food'] / events['numPassengers'], 0, events['fuelFood'],
               events['fuel_not_Food']]
    events['fuel_marginal'] = np.select(conditions, choices, default=np.nan)

    # Marginal emission
    conditions1 = [(events['modeBEAM_rh'] == 'ride_hail_pooled') & (events['fuelElectricity'].notna() != 0),
                   (events['modeBEAM_rh'] == 'ride_hail_pooled') & (events['fuelGasoline'].notna() != 0),
                   (events['modeBEAM_rh'] == 'ride_hail_pooled') & (events['fuelBiodiesel'].notna() != 0),
                   (events['modeBEAM_rh'] == 'ride_hail_pooled') & (events['fuelDiesel'].notna() != 0),
                   (events['modeBEAM_rh'] == 'walk_transit') | (events['modeBEAM_rh'] == 'drive_transit') |
                   (events['modeBEAM_rh'] == 'ride_hail_transit') | (events['modeBEAM_rh'] == 'bus') | (
                           events['modeBEAM_rh'] == 'subway') |
                   (events['modeBEAM_rh'] == 'rail') | (events['modeBEAM_rh'] == 'tram') | (
                           events['modeBEAM_rh'] == 'cable_car') |
                   (events['modeBEAM_rh'] == 'bike_transit'),

                   (events['modeBEAM_rh'] == 'walk') | (events['modeBEAM_rh'] == 'bike'),

                   (events['modeBEAM_rh'] == 'ride_hail') | (events['modeBEAM_rh'] == 'car') |
                   (events['modeBEAM_rh'] == 'car_hov2') | (events['modeBEAM_rh'] == 'car_hov3') |
                   (events['modeBEAM_rh'] == 'hov2_teleportation') | (
                           events['modeBEAM_rh'] == 'hov3_teleportation') &
                   (events['fuelElectricity'].notna() != 0),

                   (events['modeBEAM_rh'] == 'ride_hail') | (events['modeBEAM_rh'] == 'car') |
                   (events['modeBEAM_rh'] == 'car_hov2') | (events['modeBEAM_rh'] == 'car_hov3') |
                   (events['modeBEAM_rh'] == 'hov2_teleportation') | (
                           events['modeBEAM_rh'] == 'hov3_teleportation') &
                   (events['fuelGasoline'].notna() != 0),

                   (events['modeBEAM_rh'] == 'ride_hail') | (events['modeBEAM_rh'] == 'car') |
                   (events['modeBEAM_rh'] == 'car_hov2') | (events['modeBEAM_rh'] == 'car_hov3') |
                   (events['modeBEAM_rh'] == 'hov2_teleportation') | (
                           events['modeBEAM_rh'] == 'hov3_teleportation') &
                   (events['fuelBiodiesel'].notna() != 0),

                   (events['modeBEAM_rh'] == 'ride_hail') | (events['modeBEAM_rh'] == 'car') |
                   (events['modeBEAM_rh'] == 'car_hov2') | (events['modeBEAM_rh'] == 'car_hov3') |
                   (events['modeBEAM_rh'] == 'hov2_teleportation') | (
                           events['modeBEAM_rh'] == 'hov3_teleportation') &
                   (events['fuelDiesel'].notna() != 0),

                   (events['modeBEAM_rh'] == 'ride_hail') | (events['modeBEAM_rh'] == 'car') |
                   (events['modeBEAM_rh'] == 'car_hov2') | (events['modeBEAM_rh'] == 'car_hov3') |
                   (events['modeBEAM_rh'] == 'hov2_teleportation') | (
                           events['modeBEAM_rh'] == 'hov3_teleportation') &
                   (events['fuelFood'].notna() != 0)]

    choices1 = [events['emissionElectricity'] / events['numPassengers'],
                events['emissionGasoline'] / events['numPassengers'],
                events['emissionBiodiesel'] / events['numPassengers'],
                events['emissionDiesel'] / events['numPassengers'],
                0,
                events['emissionFood'],
                events['emissionElectricity'],
                events['emissionGasoline'],
                events['emissionBiodiesel'],
                events['emissionDiesel'],
                events['emissionFood']]

    events['emission_marginal'] = np.select(conditions1, choices1, default=np.nan)
    events['actEndType'] = np.where(events['type'] == 'actend', events['actType'], "")
    events['actStartType'] = np.where(events['type'] == 'actstart', events['actType'], "")
    events["tripIndex"] = events.tripId.fillna(method='ffill')
    events['mode_choice_actual_BEAM'] = events.groupby(['IDMerged', 'tripId', 'type'])['modeBEAM'].transform('last')
    events['mode_choice_planned_BEAM'] = events.groupby(['IDMerged', 'tripId', 'type'])['modeBEAM'].transform(
        'first')
    events['mode_choice_actual_BEAM'] = np.where(events['type'] != 'ModeChoice', np.nan,
                                                 events['mode_choice_actual_BEAM'])
    events['mode_choice_planned_BEAM'] = np.where(events['type'] != 'ModeChoice', np.nan,
                                                  events['mode_choice_planned_BEAM'])

    # Rename the "netCost" column
    events.rename(columns={"netCost": "cost_BEAM"}, inplace=True)
    # Replanning events = 1, the rest = 0
    events['replanning_status'] = np.where(events['type'] == 'Replanning', 1, 0)
    events['reason'].replace('nan', np.NaN)
    events['transit_bus'] = np.where(events['modeBEAM_rh'] == 'bus', 1, 0)
    events['transit_subway'] = np.where(events['modeBEAM_rh'] == 'subway', 1, 0)
    events['transit_tram'] = np.where(events['modeBEAM_rh'] == 'tram', 1, 0)
    events['transit_rail'] = np.where(events['modeBEAM_rh'] == 'rail', 1, 0)
    events['transit_cable_car'] = np.where(events['modeBEAM_rh'] == 'cable_car', 1, 0)
    events['ride_hail_pooled'] = np.where(events['modeBEAM_rh'] == 'ride_hail_pooled', 1, 0)
    return events


def _add_geometry_id_to_DataFrame(df, gdf, xcol, ycol, idColumn="geometry", df_geom='epsg:4326'):
    gdf_data = gpd.GeoDataFrame(df, geometry=gpd.points_from_xy(df[xcol], df[ycol]))
    gdf_data.set_crs(df_geom, inplace=True)
    joined = gpd.sjoin(gdf_data.to_crs('epsg:26910'), gdf.to_crs('epsg:26910'))
    gdf_data = gdf_data.merge(joined['blkgrpid'], left_index=True, right_index=True, how="left")
    gdf_data.rename(columns={'blkgrpid': idColumn}, inplace=True)
    df = pd.DataFrame(gdf_data.drop(columns='geometry'))
    #df.drop(columns=[xcol, ycol], inplace=True)
    return df.loc[~df.index.duplicated(keep='first'), :]

def _add_geometry_to_events(settings, events):
    if settings['region'] == 'austin':
        taz_id_col_in = 'GEOID'
    else:
        taz_id_col_in = 'taz1454'
    taz = get_taz_geoms(settings, taz_id_col_in=taz_id_col_in)
    processed_list = Parallel(n_jobs=cpu_count() - 1)(
        delayed(_add_geometry_id_to_DataFrame)(ev, taz, "startX", "startY", "BlockGroupStart") for ev in
        np.array_split(events, cpu_count() - 1))
    processed_list = Parallel(n_jobs=cpu_count() - 1)(
        delayed(_add_geometry_id_to_DataFrame)(ev, taz, "endX", "endY", "BlockGroupEnd") for ev in
        processed_list)
    events = pd.concat(processed_list)


    # Adding the block group information
    if settings['region'] == 'sfbay':
        gdf_labels = get_taz_labels(settings)
        # Census block groups should have 12 digits so adding 0 to the start of it to be compatible with our block group ids
        gdf_labels['bgid'] = '0' + gdf_labels['bgid'].astype(str)

        events = pd.merge(events, gdf_labels,  how='left',  left_on = ['BlockGroupEnd'], right_on = 'bgid',)
        events = pd.merge(events, gdf_labels,  how='left',  left_on = ['BlockGroupStart'], right_on = 'bgid',)

        events.rename(columns={"bgid_x":"bgid_end"}, inplace=True)
        events.rename(columns={"bgid_y":"bgid_start"}, inplace=True)
        events.rename(columns={"tractid_x":"tractid_end"}, inplace=True)
        events.rename(columns={"tractid_y":"tractid_start"}, inplace=True)
        events.rename(columns={"juris_name_x":"juris_name_end"}, inplace=True)
        events.rename(columns={"juris_name_y":"juris_name_start"}, inplace=True)
        events.rename(columns={"county_name_x":"county_name_end"}, inplace=True)
        events.rename(columns={"county_name_y":"county_name_start"}, inplace=True)
        events.rename(columns={"mpo_x":"mpo_end"}, inplace=True)
        events.rename(columns={"mpo_y":"mpo_start"}, inplace=True)

    return events


def _aggregate_on_trip(df, name):
    aggfunc = {'actStartTime': np.sum,
               'actEndTime': np.sum,
               'duration_travelling': np.sum,
               'cost_BEAM': np.sum,
               'actStartType': np.sum,
               'actEndType': np.sum,
               'duration_walking': np.sum,
               'duration_in_privateCar': np.sum,
               'duration_on_bike': np.sum,
               'duration_in_ridehail': np.sum,
               'distance_travelling': np.sum,
               'duration_in_transit': np.sum,
               'distance_walking': np.sum,
               'distance_bike': np.sum,
               'distance_ridehail': np.sum,
               'distance_privateCar': np.sum,
               'distance_transit': np.sum,
               'legVehicleIds': np.sum,
               'mode_choice_planned_BEAM': lambda x: ', '.join(set(x.dropna().astype(str))),
               'mode_choice_actual_BEAM': lambda x: ', '.join(set(x.dropna().astype(str))),
               'vehicle': lambda x: ', '.join(set(x.dropna().astype(str))),
               'numPassengers': lambda x: ', '.join(list(x.dropna().astype(str))),
               'distance_mode_choice': np.sum,
               'replanning_status': np.sum,
               'reason': lambda x: ', '.join(list(x.dropna().astype(str))),
               'parkingType': lambda x: ', '.join(list(x.dropna().astype(str))),
               'transit_bus': np.sum,
               'transit_subway': np.sum,
               'transit_tram': np.sum,
               'transit_cable_car': np.sum,
               'ride_hail_pooled': np.sum,
               'transit_rail': np.sum,
               'fuelFood': np.sum,
               'fuelElectricity': np.sum,
               'fuelBiodiesel': np.sum,
               'fuelDiesel': np.sum,
               'fuel_not_Food': np.sum,
               'fuelGasoline': np.sum,
               'fuel_marginal': np.sum,
               'BlockGroupStart': 'first',
               'startX': 'first',
               'startY': 'first',
               'bgid_start': 'first',
               'tractid_start':'first',
               'juris_name_start': 'first',
               'county_name_start': 'first',
               'mpo_start': 'first',
               'BlockGroupEnd': 'last',
               'endX': 'last',
               'endY': 'last',
               'bgid_end':'last',
               'tractid_end':'last',
               'juris_name_end':'last',
               'county_name_end': 'last',
               'mpo_end': 'last',
               'emissionFood': np.sum,
               'emissionElectricity': np.sum,
               'emissionDiesel': np.sum,
               'emissionGasoline': np.sum,
               'emissionBiodiesel': np.sum,
               'emission_marginal': np.sum
               }
    agg = df.groupby('tripIndex').agg(aggfunc)
    return pd.concat({name: agg}, names=["IDMerged"])


def _build_person_trip_events(events):
    gb = events.groupby('IDMerged')
    processed_list = Parallel(n_jobs=cpu_count() - 1)(delayed(_aggregate_on_trip)(group, name) for name, group in gb)
    person_trip_events = pd.concat(processed_list)
    return person_trip_events


def _process_person_trip_events(person_trip_events):
    person_trip_events['duration_door_to_door'] = person_trip_events['actStartTime'] - person_trip_events[
        'actEndTime']
    person_trip_events['waitTime_no_replanning'] = np.where(person_trip_events['replanning_status'] == 0,
         person_trip_events['duration_door_to_door'] - person_trip_events['duration_travelling'], 0)
    person_trip_events['waitTime_replanning'] = np.where(person_trip_events['replanning_status'] > 0,
         person_trip_events['duration_door_to_door'] - person_trip_events['duration_travelling'], 0)
    person_trip_events['actPurpose'] = person_trip_events['actEndType'].astype(str) + "_to_" + person_trip_events[
        'actStartType'].astype(str)
    person_trip_events.rename(columns={"legVehicleIds": "vehicleIds_estimate"}, inplace=True)
    person_trip_events.rename(columns={"vehicle": "vehicleIds"}, inplace=True)
    # Column with five summarized modes
    conditions = [(person_trip_events['mode_choice_actual_BEAM'] == 'ride_hail') | (
            person_trip_events['mode_choice_actual_BEAM'] == 'ride_hail_pooled'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'walk_transit') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'drive_transit') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'ride_hail_transit') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'bike_transit'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'walk'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'bike'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'car') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'car_hov2') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'car_hov3') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'hov2_teleportation') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'hov3_teleportation')]
    choices = ['ride_hail', 'transit', 'walk', 'bike', 'car']
    person_trip_events['mode_choice_actual_5'] = np.select(conditions, choices, default=np.nan)
    # Column with six summarized modes
    conditions = [(person_trip_events['mode_choice_actual_BEAM'] == 'ride_hail') | (
            person_trip_events['mode_choice_actual_BEAM'] == 'ride_hail_pooled'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'walk_transit') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'drive_transit') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'bike_transit'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'walk'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'bike'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'car') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'car_hov2') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'car_hov3') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'hov2_teleportation') | (
                          person_trip_events['mode_choice_actual_BEAM'] == 'hov3_teleportation'),
                  (person_trip_events['mode_choice_actual_BEAM'] == 'ride_hail_transit')]
    choices = ['ride_hail', 'transit', 'walk', 'bike', 'car', 'ride_hail_transit']
    person_trip_events['mode_choice_actual_6'] = np.select(conditions, choices, default=np.nan)
    # Column with four summarized modes
    person_trip_events['mode_choice_actual_4']  = np.where((person_trip_events['mode_choice_actual_5'] == 'walk')|(
                          person_trip_events['mode_choice_actual_5'] == 'bike'), 'walk/bike', person_trip_events['mode_choice_actual_5'])
    return person_trip_events.sort_values(by=['IDMerged', 'tripIndex']).reset_index(drop=False)


def _read_asim_utilities(settings, year, iteration):
    asim_output_data_dir = settings['asim_local_output_folder']
    iteration_output_dir = "year-{0}-iteration-{1}".format(year, iteration)
    trip_utility_location = os.path.join(asim_output_data_dir, iteration_output_dir, "trip_mode_choice.zip")
    chunks = []
    with zipfile.ZipFile(trip_utility_location) as z:
        for filename in z.namelist():
            if not os.path.isdir(filename):
                if filename.endswith("utilities.csv"):
                    chunks.append(pd.read_csv(z.open(filename)))
    return pd.concat(chunks, ignore_index=True).sort_values(by=['trip_id'])


def _merge_trips_with_utilities(asim_trips, asim_utilities, beam_trips):
    tripIdsBEAM = set(beam_trips.tripIndex)
    tripIdsASIM = set(asim_trips.trip_id)
    logger.info("Finding {0} BEAM trips not in ASIM plans and {1} ASIM trips not in BEAM events".format(
        len(tripIdsBEAM - tripIdsASIM), len(tripIdsASIM - tripIdsBEAM)))
    SFActMerged = pd.merge(left=asim_trips, right=asim_utilities, how='left', on=['trip_id']).sort_values(
        by=['person_id', 'trip_id']).reset_index(drop=True)
    eventsASim = pd.merge(left=beam_trips, right=SFActMerged, how='left', left_on=["IDMerged", 'tripIndex'],
                          right_on=['person_id', 'trip_id'])
    eventsASim.rename(columns={"mode_choice_logsum_y": "logsum_tours_mode_AS_tours"}, inplace=True)
    eventsASim.rename(columns={"tour_mode": "tour_mode_AS_tours"}, inplace=True)
    eventsASim.rename(columns={"mode_choice_logsum_x": "logsum_trip_Potential_INEXUS"}, inplace=True)
    eventsASim.rename(columns={"trip_mode": "trip_mode_AS_trips"}, inplace=True)

    # Add a column of income quartiles
    quartiles = eventsASim['income'].quantile([0,.25, .5, .75,1]).tolist()
    # Add income deciles
    conditions  = [(eventsASim['income'] >= quartiles[0]) & (eventsASim['income'] < quartiles[1]),
                   (eventsASim['income'] >= quartiles[1]) & (eventsASim['income'] < quartiles[2]),
                   (eventsASim['income'] >=  quartiles[2]) & (eventsASim['income'] < quartiles[3]),
                   (eventsASim['income'] >= quartiles[3]) & (eventsASim['income'] <= quartiles[4])]

    choices = [ '1stQ', '2ndQ', '3rdQ', '4thD']
    eventsASim['income_quartiles'] = np.select(conditions, choices, default=None)
    # Add a column of income deciles
    deciles = eventsASim['income'].quantile([0,0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9,1]).tolist()
    # Add income deciles
    conditions  = [(eventsASim['income'] >= deciles[0]) & (eventsASim['income'] < deciles[1]),
                   (eventsASim['income'] >= deciles[1]) & (eventsASim['income'] < deciles[2]),
                   (eventsASim['income'] >=  deciles[2]) & (eventsASim['income'] < deciles[3]),
                   (eventsASim['income'] >= deciles[3]) & (eventsASim['income'] < deciles[4]),
                   (eventsASim['income'] >=  deciles[4]) & (eventsASim['income'] < deciles[5]),
                   (eventsASim['income'] >=  deciles[5]) & (eventsASim['income'] < deciles[6]),
                   (eventsASim['income'] >=  deciles[6]) & (eventsASim['income'] < deciles[7]),
                   (eventsASim['income'] >=  deciles[7]) & (eventsASim['income'] < deciles[8]),
                   (eventsASim['income'] >=  deciles[8]) & (eventsASim['income'] < deciles[9]),
                   (eventsASim['income'] >=  deciles[9]) & (eventsASim['income'] <= deciles[10])]

    choices = [ '1stD', '2ndD', '3rdD',
               '4thD', '5thD', '6thD', '7thD', '8thD', '9thD','10thD']

    eventsASim['income_deciles'] = np.select(conditions, choices, default=None)

    return eventsASim


def _read_asim_plans(settings, year, iteration):
    asim_output_data_dir = settings['asim_local_output_folder']
    iteration_output_dir = "year-{0}-iteration-{1}".format(year, iteration)
    path = os.path.join(asim_output_data_dir, iteration_output_dir)
    households = pd.read_csv(os.path.join(path, "households.csv.gz")).sort_values(by=['household_id']).reset_index(
        drop=True)
    persons = pd.read_csv(os.path.join(path, "persons.csv.gz")).sort_values(by=['household_id']).reset_index(drop=True)
    tours = pd.read_csv(os.path.join(path, "final_tours.csv.gz")).sort_values(by=['person_id']).reset_index(drop=True)
    trips = pd.read_csv(os.path.join(path, "final_trips.csv.gz")).sort_values(by=['person_id', 'tour_id']).reset_index(
        drop=True)
    hhpersons = pd.merge(left=persons, right=households, how='left', on='household_id')
    hhperTours = pd.merge(left=tours, right=hhpersons, how='left', on='person_id').sort_values(
        by=['person_id', 'tour_id']).reset_index(drop=True)
    tour_trips = pd.merge(left=trips, right=hhperTours, how='left', on=['person_id', 'tour_id']).sort_values(
        by=['trip_id'])
    return tour_trips


def build_mep_summaries(trips, settings, year, iteration):
    totalsByMode = trips.loc[:,
                   ['mode_choice_actual_BEAM', 'cost_BEAM', 'distance_mode_choice', 'fuel_marginal']].groupby(
        'mode_choice_actual_BEAM').agg(sum)
    totalsByMode.index = totalsByMode.index.str.replace("teleportation", "passenger")
    totalsByMode['cost_per_passenger_mile'] = totalsByMode['cost_BEAM'] / totalsByMode['distance_mode_choice'] * 1609.34
    totalsByMode['joules_per_passenger_mile'] = totalsByMode['fuel_marginal'] / totalsByMode[
        'distance_mode_choice'] * 1609.34
    beam_output_dir = settings['beam_local_output_folder']
    region = settings['region']
    iteration_output_dir = "year-{0}-iteration-{1}".format(year, iteration)
    totalsByMode.to_csv(os.path.join(beam_output_dir, region, iteration_output_dir, "totalsByMode.csv"))


def process_event_file(settings, year, iteration):
    try:
        logger.info("Loading utilities")
        utils = _read_asim_utilities(settings, year, iteration)
        logger.info("Loading events")
        events = _load_events_file(settings, year, iteration)
        events = _reformat_events_file(events)
        # TODO: get cost and energy per passenger mile for the different modes
        logger.info("Adding geoms to events")
        events = _add_geometry_to_events(settings, events)
        logger.info("Expanding events")
        events = _expand_events_file(events)
        logger.info("Building person trip events")
        person_trip_events = _build_person_trip_events(events)
        del events
        person_trip_events = _process_person_trip_events(person_trip_events)
        logger.info("Reading asim plans")
        tour_trips = _read_asim_plans(settings, year, iteration)
        logger.info("Merging final outputs")
        try:
            final_output = _merge_trips_with_utilities(tour_trips, utils, person_trip_events)
        except Exception as e:
            print("Error during merging: \n {0}".format(e))
            logger.error("Error during merging: \n {0}".format(e))
        scenario_defs = settings['scenario_definitions']
        post_output_folder = settings['postprocessing_output_folder']

        filename = "{0}_{1}_{2}-{3}_{4}__{5}.csv.gz".format(settings['region'],
                                                            scenario_defs['name'],
                                                            scenario_defs['lever'],
                                                            scenario_defs['lever_position'],
                                                            year,
                                                            date.today().strftime("%Y%m%d"))
        final_output.to_csv(os.path.join(post_output_folder, filename), compression="gzip")

        logger.info("Building mep summaries")
        try:
            build_mep_summaries(final_output, settings, year, iteration)
        except Exception as e:
            print("Error during mep summary: \n {0}".format(e))

    except Exception as e:
        logger.error("Did not successfully run the postproccessor, did activitysim fail? \n {0}".format(e))


if __name__ == '__main__':
    os.chdir("../..")
    settings = parse_args_and_settings(os.path.join("settings.yaml"))
    beam_output_dir = settings['beam_local_output_folder']
    region = settings['region']
    output_path = os.path.join(beam_output_dir, region, "year*")
    outputDirs = glob.glob(output_path)
    yearsAndIters = [(loc.split('-', 3)[-3], loc.split('-', 3)[-1]) for loc in outputDirs]
    yrs = dict()
    # Only do this for the latest available iteration in each year
    for year, iter in yearsAndIters:
        if year in yrs:
            if int(iter) > int(yrs[year]):
                yrs[year] = iter
        else:
            yrs[year] = iter
    for year, iter in yrs.items():
        process_event_file(settings, year, iter)
        copy_outputs_to_mep(settings, year, iter)