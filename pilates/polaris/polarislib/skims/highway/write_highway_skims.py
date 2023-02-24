from ..utils.write_mat import write_mat
from ..utils.writes_skim_headers import write_skim_headers


def write_highway_skim(infile, matrices, index, intervals):
    write_skim_headers(infile, index, intervals, None)

    for interv in intervals:
        for metric in ["time", "distance", "cost"]:
            write_mat(infile, matrices[metric][interv])
