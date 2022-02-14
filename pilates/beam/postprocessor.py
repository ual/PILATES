import pandas as pd
import os

def find_latest_beam_iteration(beam_output_dir):
    iter_dirs = [os.path.join(root, dir) for root, dirs, files in os.walk(beam_output_dir) for dir in dirs if
                 dir == "ITERS"]
    if not iter_dirs:
        return None, None
    last_iters_dir = max(iter_dirs, key=os.path.getmtime)
    all_iteration_dir = [it for it in os.listdir(last_iters_dir)]
    if not all_iteration_dir:
        return None, None
    it_prefix = "it."
    max_it_num = max(dir_name[len(it_prefix):] for dir_name in all_iteration_dir)
    return os.path.join(last_iters_dir, it_prefix + str(max_it_num)), max_it_num


def find_produced_skims(beam_output_dir):
    iteration_dir, it_num = find_latest_beam_iteration(beam_output_dir)
    if iteration_dir is None:
        return None
    skims_path = os.path.join(iteration_dir, f"{it_num}.activitySimODSkims_current.csv.gz")
    if os.path.exists(skims_path):
        return skims_path
    else:
        return None


def merge_current_skims(all_skims_path, previous_skims_path, beam_output_dir):
    current_skims_path = find_produced_skims(beam_output_dir)
    if (current_skims_path is None) | (previous_skims_path == current_skims_path):
        # this means beam has not produced the skims
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
