import struct

import numpy as np

from ..utils.check_tag import Check_Tag


def load_transit_skims(infile, zones, modes, metrics, bus_only, infinite):
    # read intervals
    if not Check_Tag(infile, "BINT"):
        return
    num_intervals = struct.unpack("i", infile.read(4))[0]
    intervals = [struct.unpack("i", infile.read(4))[0] for i in range(num_intervals)]
    if not Check_Tag(infile, "EINT"):
        return

    tsize = zones * zones
    # for each interval, check tags and read in matrix
    mat_store = {met: {md: {it: [] for it in intervals} for md in modes} for met in metrics}

    def read_mat(infile, mat_name):
        if not Check_Tag(infile, "BMAT"):
            raise RuntimeError(f"check-tag-BMAT-failed for interval {mat_name}")
        data = np.fromfile(infile, dtype="f", count=tsize).astype(float)
        if data.size < tsize or not Check_Tag(infile, "EMAT"):
            raise RuntimeError(f"check-tag-EMAT-failed for interval for {mat_name}")

        data[data >= infinite] = np.nan
        return data.reshape(zones, zones)

    for inter in intervals:
        for mode in modes:
            if mode != "BUS" and bus_only:
                return mat_store
            for metr in metrics:
                mat_name = f"{inter} {mode}"
                mat_store[metr][mode][inter] = read_mat(infile, mat_name)
    return mat_store
