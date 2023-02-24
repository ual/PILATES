from ..utils.write_mat import write_mat
from ..utils.writes_skim_headers import write_skim_headers


def write_transit_skim(infile, modes, metrics, matrices, index, intervals):
    write_skim_headers(infile, index, intervals, modes)

    for interval in intervals:
        for md in modes:
            for metr in metrics:
                write_mat(infile, matrices[metr][md][interval])
