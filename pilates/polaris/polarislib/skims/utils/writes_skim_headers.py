import numpy as np

from .write_tag import write_tag


def write_skim_headers(infile, index, intervals, modes):
    write_tag(infile, "SKIM:V03")

    if modes is not None:
        write_tag(infile, "MODE")
        np.array([len(modes) - 1], np.int32).tofile(infile)

    # Shapes and zones
    write_tag(infile, "BZON")
    np.array([index.shape[0]], np.int32).tofile(infile)
    np.array(index.zones.values, np.int64).tofile(infile)
    write_tag(infile, "EZON")

    # Writes interval information
    write_tag(infile, "BINT")
    np.array([len(intervals)], np.int32).tofile(infile)
    np.array(intervals, np.int32).tofile(infile)
    write_tag(infile, "EINT")
