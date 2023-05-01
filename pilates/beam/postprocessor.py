import pandas as pd
import os
import logging
import openmatrix as omx
import numpy as np
# import pickle
# import cloudpickle
# import dill
#
# np.seterr(divide='ignore')
#
# dill.settings['recurse'] = True
#
# pickle.ForkingPickler = cloudpickle.Pickler
#
# import multiprocessing as mp

# mp.set_start_method('spawn', True)
# from multiprocessing import Pool, cpu_count
from joblib import Parallel, delayed

logger = logging.getLogger(__name__)


def find_latest_beam_iteration(beam_output_dir):
    iter_dirs = [os.path.join(root, dir) for root, dirs, files in os.walk(beam_output_dir) if
                 not root.startswith('.') for dir in dirs if dir == "ITERS"]
    logger.info("Looking in directories {0}".format(iter_dirs))
    if not iter_dirs:
        return None, None
    last_iters_dir = max(iter_dirs, key=os.path.getmtime)
    all_iteration_dir = [it for it in os.listdir(last_iters_dir) if not it.startswith('.')]
    logger.info("Looking in directories {0}".format(all_iteration_dir))
    if not all_iteration_dir:
        return None, None
    it_prefix = "it."
    max_it_num = max(dir_name[len(it_prefix):] for dir_name in all_iteration_dir)
    return os.path.join(last_iters_dir, it_prefix + str(max_it_num)), max_it_num


def rename_beam_output_directory(settings, year, replanning_iteration_number=0):
    beam_output_dir = settings['beam_local_output_folder']
    iteration_output_directory, _ = find_latest_beam_iteration(beam_output_dir)
    beam_run_output_dir = os.path.join(*iteration_output_directory.split(os.sep)[:-2])
    new_iteration_output_directory = os.path.join(beam_output_dir, settings['region'],
                                                  "year-{0}-iteration-{1}".format(year, replanning_iteration_number))
    os.rename(beam_run_output_dir, new_iteration_output_directory)


def find_produced_od_skims(beam_output_dir, suffix="csv.gz"):
    iteration_dir, it_num = find_latest_beam_iteration(beam_output_dir)
    if iteration_dir is None:
        return None
    od_skims_path = os.path.join(iteration_dir, "{0}.activitySimODSkims_current.{1}".format(it_num, suffix))
    logger.info("expecting skims at {0}".format(od_skims_path))
    return od_skims_path


def reorder(path):
    split = path.split('_')
    if len(split) == 6:
        split[3], split[5] = split[5], split[3]
        return '_'.join(split)
    elif len(split) == 4:
        split[1], split[3] = split[3], split[1]
        return '_'.join(split)
    elif len(split) == 5:
        split[2], split[4] = split[4], split[2]
        return '_'.join(split)
    else:
        print('STOP ', path)


def find_produced_origin_skims(beam_output_dir):
    iteration_dir, it_num = find_latest_beam_iteration(beam_output_dir)
    if iteration_dir is None:
        return None
    ridehail_skims_path = os.path.join(iteration_dir, f"{it_num}.skimsRidehail.csv.gz")
    logger.info("expecting skims at {0}".format(ridehail_skims_path))
    return ridehail_skims_path


def _merge_skim(inputMats, outputMats, path, timePeriod, measures):
    complete_key = '_'.join([path, 'TRIPS', '', timePeriod])
    failed_key = '_'.join([path, 'FAILURES', '', timePeriod])
    completed, failed = None, None
    if complete_key in inputMats.keys():
        completed = np.array(inputMats[complete_key])
        failed = np.array(inputMats[failed_key])
        logger.info("Adding {0} valid trips and {1} impossible trips to skim {2}, where {3} had existed before".format(
            np.nan_to_num(completed).sum(),
            np.nan_to_num(failed).sum(),
            complete_key,
            np.nan_to_num(np.array(outputMats[complete_key])).sum()))
        toPenalize = np.array([0])
        for measure in measures:
            inputKey = '_'.join([path, measure, '', timePeriod])
            if path in ["WALK", "BIKE"]:
                if measure == "DIST":
                    outputKey = path + "DIST"
                else:
                    outputKey = '_'.join([path, measure])
            else:
                outputKey = inputKey
            if (outputKey in outputMats) and (inputKey in inputMats):
                if measure == "TRIPS":
                    outputMats[outputKey][completed > 0] += inputMats[inputKey][completed > 0]
                elif measure == "FAILURES":
                    outputMats[outputKey][failed > 0] += inputMats[inputKey][failed > 0]
                elif measure == "DIST":
                    outputMats[outputKey][completed > 0] = 0.5 * (
                            outputMats[outputKey][completed > 0] + inputMats[inputKey][completed > 0])
                elif measure in ["IWAIT", "XWAIT", "WACC", "WAUX", "WEGR", "DTIM", "DDIST", "FERRYIVT"]:
                    # NOTE: remember the mtc asim implementation has scaled units for these variables
                    outputMats[outputKey][completed > 0] = inputMats[inputKey][completed > 0] * 100.0
                elif measure == "TOTIVT":
                    inputKeyKEYIVT = '_'.join([path, 'KEYIVT', '', timePeriod])
                    outputKeyKEYIVT = inputKeyKEYIVT
                    if (inputKeyKEYIVT in inputMats.keys()) & (outputKeyKEYIVT in outputMats.keys()):
                        additionalFilter = (outputMats[outputKeyKEYIVT][:] > 0)
                    else:
                        additionalFilter = False

                    toCancel = (failed > 10) & (failed > 2 * completed) & (
                            (outputMats[outputKey][:] > 0) | additionalFilter)
                    # save this for later so it doesn't get overwritten
                    toPenalize = (failed > completed) & ~toCancel & (
                            (outputMats[outputKey][:] > 0) | additionalFilter)
                    if toCancel.sum() > 0:
                        logger.info(
                            "Marking {0} {1} trips completely impossible in {2}. There were {3} completed trips but {4}"
                            " failed trips in these ODs".format(
                                toCancel.sum(), path, completed[toCancel].sum(), failed[toCancel].sum(), timePeriod))
                    toAllow = ~toCancel & ~toPenalize
                    outputMats[outputKey][toCancel] = 0.0
                    outputMats[outputKey][toAllow] = inputMats[inputKey][toAllow] * 100
                    if (inputKeyKEYIVT in inputMats.keys()) & (outputKeyKEYIVT in outputMats.keys()):
                        outputMats[outputKeyKEYIVT][toCancel] = 0.0
                        outputMats[outputKeyKEYIVT][toAllow] = inputMats[inputKeyKEYIVT][toAllow] * 100
                elif ~measure.endswith("TOLL"):  # hack to avoid overwriting initial tolls
                    outputMats[outputKey][completed > 0] = inputMats[inputKey][completed > 0]
                    if path.startswith('SOV_'):
                        for sub in ['SOVTOLL_', 'HOV2_', 'HOV2TOLL_', 'HOV3_', 'HOV3TOLL_']:
                            newKey = '_'.join([path, measure.replace('SOV_', sub), '', timePeriod])
                            outputMats[newKey][completed > 0] = inputMats[inputKey][completed > 0]
                            logger.info("Adding {0} valid trips and {1} impossible trips to skim {2}".format(
                                np.nan_to_num(completed).sum(),
                                np.nan_to_num(failed).sum(),
                                newKey))
            elif outputKey in outputMats:
                logger.warning("Target skims are missing key {0}".format(outputKey))
            else:
                logger.warning("BEAM skims are missing key {0}".format(outputKey))

        if ("TOTIVT" in measures) & ("IWAIT" in measures) & ("KEYIVT" in measures):
            if toPenalize.sum() > 0:
                inputKey = '_'.join([path, 'IWAIT', '', timePeriod])
                outputMats[inputKey][toPenalize] = inputMats[inputKey][toPenalize] * (failed[toPenalize] + 1) / (
                        completed[toPenalize] + 1)
    # outputSkim.close()
    # inputSkim.close()
    else:
        logger.info(
            "No input skim for mode {0} and time period {1}, with key {2}".format(path, timePeriod, complete_key))
    return (path, timePeriod), (completed, failed)


def simplify(input, timePeriod, mode, utf=False, expand=False):
    # TODO: This is a hack
    hdf_utf = input[{"mode": mode.encode('utf-8'), "timePeriod": timePeriod.encode('utf-8')}]
    hdf = input[{"mode": mode, "timePeriod": timePeriod}]
    originalDictUtf = {sk.name: sk for sk in hdf}
    originalDict = {sk.name: sk for sk in hdf_utf}
    bruteForceDict = {name: input[name] for name in input.list_matrices() if
                      (name.startswith(mode) & name.endswith(timePeriod))}
    if originalDict is None:
        if originalDictUtf is None:
            originalDict = bruteForceDict
        else:
            originalDict = originalDictUtf.update(originalDictUtf)
    else:
        originalDict.update(bruteForceDict)
        if originalDictUtf is not None:
            originalDict.update(originalDictUtf)
    newDict = dict()
    if expand:
        for key, item in originalDict.items():
            if key.startswith('SOV_'):
                newKey = key.replace('SOV_', 'SOVTOLL_')
                newDict[newKey] = item
                newKey = key.replace('SOV_', 'HOV2_')
                newDict[newKey] = item
                newKey = key.replace('SOV_', 'HOV2TOLL_')
                newDict[newKey] = item
                newKey = key.replace('SOV_', 'HOV3_')
                newDict[newKey] = item
                newKey = key.replace('SOV_', 'HOV3TOLL_')
                newDict[newKey] = item
    originalDict.update(newDict)
    return originalDict


def copy_skims_for_unobserved_modes(mapping, skims):
    for fromMode, toModes in mapping.items():
        relevantSkimKeys = [key for key in skims.list_matrices() if key.startswith(fromMode + "_") & ~("TOLL" in key)]
        for skimKey in relevantSkimKeys:
            for toMode in toModes:
                toKey = skimKey.replace(fromMode + "_", toMode + "_")
                skims[toKey][:] = skims[skimKey][:]
                print("Copying values from {0} to {1}".format(skimKey, toKey))


def merge_current_omx_od_skims(all_skims_path, previous_skims_path, beam_output_dir):
    skims = omx.open_file(all_skims_path, 'a')
    current_skims_path = find_produced_od_skims(beam_output_dir, "omx")
    partialSkims = omx.open_file(current_skims_path, mode='r')
    iterable = [(
        path, timePeriod, vals[1].to_list()) for (path, timePeriod), vals
        in
        pd.Series(partialSkims.listMatrices()).str.rsplit('_', n=3, expand=True).groupby([0, 3])]

    # GIVING UP ON PARALLELIZING THIS FOR NOW. see below for attempts that didn't work for some reason or another

    # results = Parallel(n_jobs=-1)(delayed(_merge_skim)(x) for x in iterable)
    # p = mp.Pool(4)
    # result = [p.apply_async(_merge_skim, args=((simplify(partialSkims, timePeriod, path, True),
    #                                             simplify(skims, timePeriod, path, False), path,
    #                                             timePeriod, vals))) for (path, timePeriod, vals) in iterable]
    result = [_merge_skim(simplify(partialSkims, timePeriod, path, True),
                          simplify(skims, timePeriod, path, False), path,
                          timePeriod, vals) for (path, timePeriod, vals) in iterable]

    discover_impossible_ods(result, skims)
    mapping = {"SOV": ["SOVTOLL", "HOV2", "HOV2TOLL", "HOV3", "HOV3TOLL"]}
    copy_skims_for_unobserved_modes(mapping, skims)
    skims.close()
    partialSkims.close()
    return current_skims_path


def discover_impossible_ods(result, skims):
    timePeriods = np.unique([b for (a, b), _ in result])
    # WALK TRANSIT:
    for tp in timePeriods:
        completed = {(a, b): c for (a, b), (c, d) in result if a.startswith('WLK') & a.endswith('WLK') & (b == tp)}
        failed = {(a, b): c for (a, b), (c, d) in result if a.startswith('WLK') & a.endswith('WLK') & (b == tp)}
        totalCompleted = np.nansum(list(completed.values()), axis=0)
        totalFailed = np.nansum(list(failed.values()), axis=0)
        for (path, _), mat in completed.items():
            name = '_'.join([path, 'TOTIVT', '', tp])
            toDelete = (mat == 0) & (totalCompleted > 50) & (totalFailed > 50) & (skims[name][:] > 0)
            if np.any(toDelete):
                print(
                    "Deleting {0} ODs for {1} in the {2} because after 50 transit trips "
                    "there no one has chosen it".format(
                        toDelete.sum(), path, tp))
                skims[name][toDelete] = 0


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

    all_skims = pd.read_csv(all_skims_path, dtype=schema, index_col=index_columns, na_values=["∞"])
    cur_skims = pd.read_csv(current_skims_path, dtype=schema, index_col=index_columns, na_values=["∞"])
    for col in cur_skims.columns:  # Handle new skim columns
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
    if df['completedRequests'].sum() > 0:
        totalCompletedRequests = df['completedRequests'].sum()
        waitTime = (df['waitTime'] * df['completedRequests']).sum() / totalCompletedRequests / 60.
        costPerMile = (df['costPerMile'] * df['completedRequests']).sum() / totalCompletedRequests
        observations = df['observations'].sum()
        unmatchedRequestPortion = 1. - totalCompletedRequests / observations
        return pd.Series({"waitTimeInMinutes": waitTime, "costPerMile": costPerMile,
                          "unmatchedRequestPortion": unmatchedRequestPortion, "observations": observations,
                          "completedRequests": totalCompletedRequests})
    else:
        observations = df['observations'].sum()
        return pd.Series({"waitTimeInMinutes": 6.0, "costPerMile": 5.0,
                          "unmatchedRequestPortion": 1.0, "observations": observations,
                          "completedRequests": 0})


def merge_current_omx_origin_skims(all_skims_path, previous_skims_path, beam_output_dir, measure_map):
    current_skims_path = find_produced_origin_skims(beam_output_dir)

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

    cur_skims = pd.read_csv(current_skims_path, dtype=rawInputSchema, na_values=["∞"])
    cur_skims['timePeriod'] = cur_skims['hour'].apply(hourToTimeBin)
    cur_skims.rename(columns={'tazId': 'origin'}, inplace=True)
    cur_skims['completedRequests'] = cur_skims['observations'] * (1. - cur_skims['unmatchedRequestsPercent'] / 100.)
    cur_skims = cur_skims.groupby(['timePeriod', 'reservationType', 'origin']).apply(aggregateInTimePeriod)
    cur_skims['failures'] = cur_skims['observations'] - cur_skims['completedRequests']
    skims = omx.open_file(all_skims_path, 'a')
    idx = pd.Index(np.array(list(skims.mapping("zone_id").keys()), dtype=str).copy())
    for (timePeriod, reservationType), _df in cur_skims.groupby(level=[0, 1]):
        df = _df.loc[(timePeriod, reservationType)].reindex(idx, fill_value=0.0)

        trips = "RH_{0}_{1}__{2}".format(reservationType.upper(), 'TRIPS', timePeriod.upper())
        failures = "RH_{0}_{1}__{2}".format(reservationType.upper(), 'FAILURES', timePeriod.upper())
        rejectionprob = "RH_{0}_{1}__{2}".format(reservationType.upper(), 'REJECTIONPROB', timePeriod.upper())

        logger.info(
            "Adding {0} complete trips and {1} failed trips to skim {2}".format(int(df['completedRequests'].sum()),
                                                                                int(df['failures'].sum()),
                                                                                trips))

        skims[trips][:] = skims[trips][:] * 0.5 + df.loc[:, 'completedRequests'].values[:, None]
        skims[failures][:] = skims[trips][:] * 0.5 + df.loc[:, 'failures'].values[:, None]
        skims[rejectionprob][:] = np.nan_to_num(skims[failures][:] / (skims[trips][:] + skims[failures][:]))

        wait = "RH_{0}_{1}__{2}".format(reservationType.upper(), 'WAIT', timePeriod.upper())
        originalWaitTime = np.array(
            skims[wait])  # Hack due to pytables issue https://github.com/PyTables/PyTables/issues/310
        originalWaitTime[df['completedRequests'] > 0, :] = df.loc[
                                                               df['completedRequests'] > 0, measure_map['WAIT']].values[
                                                           :,
                                                           None]
        skims[wait][:] = originalWaitTime


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

    all_skims = pd.read_csv(all_skims_path, dtype=aggregatedInput, na_values=["∞"])
    all_skims.set_index(index_columns, drop=True, inplace=True)
    cur_skims = pd.read_csv(current_skims_path, dtype=rawInputSchema, na_values=["∞"])
    cur_skims['timePeriod'] = cur_skims['hour'].apply(hourToTimeBin)
    cur_skims.rename(columns={'tazId': 'origin'}, inplace=True)
    cur_skims['completedRequests'] = cur_skims['observations'] * (1. - cur_skims['unmatchedRequestsPercent'] / 100.)
    cur_skims = cur_skims.groupby(['timePeriod', 'reservationType', 'origin']).apply(aggregateInTimePeriod)
    all_skims = pd.concat([cur_skims, all_skims.loc[all_skims.index.difference(cur_skims.index, sort=False)]])
    if all_skims.index.duplicated().sum() > 0:
        logger.warning("Duplicated values in index: \n {0}".format(all_skims.loc[all_skims.duplicated()]))
        all_skims.drop_duplicates(inplace=True)
    all_skims.to_csv(all_skims_path, index=True)
    cur_skims['totalWaitTimeInMinutes'] = cur_skims['waitTimeInMinutes'] * cur_skims['completedRequests']
    totals = cur_skims.groupby(['timePeriod', 'reservationType']).sum()
    totals['matchedPercent'] = totals['completedRequests'] / totals['observations']
    totals['meanWaitTimeInMinutes'] = totals['totalWaitTimeInMinutes'] / totals['completedRequests']
    logger.info("Ridehail matching summary: \n {0}".format(totals[['meanWaitTimeInMinutes', 'matchedPercent']]))
    logger.info("Total requests: \n {0}".format(totals['observations'].sum()))
    logger.info("Total completed requests: \n {0}".format(totals['completedRequests'].sum()))
