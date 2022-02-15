import pandas as pd
import os
import logging

logger = logging.getLogger(__name__)

def find_latest_beam_iteration(beam_output_dir):
    iter_dirs = [os.path.join(root, dir) for root, dirs, files in os.walk(beam_output_dir) for dir in dirs if
                 dir == "ITERS"]
    logger.info("Looking in directories {0}".format(iter_dirs))
    if not iter_dirs:
        return None, None
    last_iters_dir = max(iter_dirs, key=os.path.getmtime)
    all_iteration_dir = [it for it in os.listdir(last_iters_dir)]
    logger.info("Looking in directories {0}".format(all_iteration_dir))
    if not all_iteration_dir:
        return None, None
    it_prefix = "it."
    max_it_num = max(dir_name[len(it_prefix):] for dir_name in all_iteration_dir)
    return os.path.join(last_iters_dir, it_prefix + str(max_it_num)), max_it_num


def find_produced_od_skims(beam_output_dir):
    iteration_dir, it_num = find_latest_beam_iteration(beam_output_dir)
    if iteration_dir is None:
        return None
    od_skims_path = os.path.join(iteration_dir, f"{it_num}.activitySimODSkims_current.csv.gz")
    logger.info("expecting skims at {0}".format(od_skims_path))
    return od_skims_path


def find_produced_origin_skims(beam_output_dir):
    iteration_dir, it_num = find_latest_beam_iteration(beam_output_dir)
    if iteration_dir is None:
        return None
    ridehail_skims_path = os.path.join(iteration_dir, f"{it_num}.skimsRidehail.csv.gz") 
    logger.info("expecting skims at {0}".format(ridehail_skims_path))
    return ridehail_skims_path


def merge_current_od_skims(all_skims_path, previous_skims_path, beam_output_dir):
    current_skims_path = find_produced_od_skims(beam_output_dir)
    if (current_skims_path is None) | (previous_skims_path == current_skims_path):
        # this means beam has not produced the skims
        logger.error("No skims found in directory {0}, defaulting to {1}".format(beam_output_dir, current_skims_path))
        return previous_skims_path

    schema = {
        "origin": str,
        "destination": str,
        "DEBUG_TEXT": str,
    }
    index_columns = ['timePeriod', 'pathType', 'origin', 'destination']

    all_skims = pd.read_csv(all_skims_path, dtype=schema, index_col=index_columns)
    cur_skims = pd.read_csv(current_skims_path, dtype=schema, index_col=index_columns)
    for col in cur_skims.columns: # Handle new skim columns
        if col not in all_skims.columns:
            all_skims[col] = 0.0
    all_skims = pd.concat([cur_skims, all_skims.loc[all_skims.index.difference(cur_skims.index, sort=False)]])
    all_skims = all_skims.reset_index()
    all_skims.to_csv(all_skims_path, index=False)
    return current_skims_path


def hourToTimeBin(hour: int):
    if hour < 3:
        return 'EV'
    elif hour < 6:
        return 'EA'
    elif hour < 10:
        return 'AM'
    elif hour < 15:
        return 'MD'
    elif hour < 19:
        return 'PM'
    else:
        return 'EV'


def aggregateInTimePeriod(df):
    completedRequests = df['observations'] * (1. - df['unmatchedRequestsPercent'] / 100.)
    if completedRequests.sum() > 0:
        totalCompletedRequests = completedRequests.sum()
        waitTime = (df['waitTime'] * completedRequests).sum() / totalCompletedRequests / 60.
        costPerMile = (df['costPerMile'] * completedRequests).sum() / totalCompletedRequests
        observations = df['observations'].sum()
        unmatchedRequestPortion = 1. - totalCompletedRequests / observations
        return pd.Series({"waitTimeInMinutes": waitTime, "costPerMile": costPerMile,
                "unmatchedRequestPortion": unmatchedRequestPortion, "observations": observations})
    else:
        observations = df['observations'].sum()
        return pd.Series({"waitTimeInMinutes": 6.0, "costPerMile": 5.0,
                          "unmatchedRequestPortion": 1.0, "observations": observations})


def merge_current_origin_skims(all_skims_path, previous_skims_path, beam_output_dir):
    current_skims_path = find_produced_origin_skims(beam_output_dir)
    if (current_skims_path is None) | (previous_skims_path == current_skims_path):
        # this means beam has not produced the skims
        logger.error("no skims produced from path {0}".format(current_skims_path))
        return previous_skims_path

    rawInputSchema = {
        "tazId": str,
        "hour": int,
        "reservationType": str,
        "waitTime": float,
        "costPerMile": float,
        "unmatchedRequestsPercent": float,
        "observations": int,
        "iterations": int
    }

    aggregatedInput = {
        "origin": str,
        "timePeriod": str,
        "reservationType": str,
        "waitTimeInMinutes": float,
        "costPerMile": float,
        "unmatchedRequestPortion": float,
        "observations": int
    }

    index_columns = ['timePeriod', 'reservationType', 'origin']

    all_skims = pd.read_csv(all_skims_path, dtype=aggregatedInput)
    all_skims.set_index(index_columns, drop=True, inplace=True)
    cur_skims = pd.read_csv(current_skims_path, dtype=rawInputSchema)
    cur_skims['timePeriod'] = cur_skims['hour'].apply(hourToTimeBin)
    cur_skims.rename(columns={'tazId':'origin'}, inplace=True)
    cur_skims = cur_skims.groupby(['timePeriod', 'reservationType', 'origin']).apply(aggregateInTimePeriod)
    all_skims = pd.concat([cur_skims, all_skims.loc[all_skims.index.difference(cur_skims.index, sort=False)]])
    if all_skims.index.duplicated().sum() > 0:
        logger.warning("Duplicated values in index: {0}".format(all_skims.loc[all_skims.duplicated()]))
        all_skims.drop_duplicates(inplace=True)
    all_skims.to_csv(all_skims_path, index=True)
