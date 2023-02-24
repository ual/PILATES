import struct

import numpy as np

from ..utils.check_tag import Check_Tag


def read_highway_skim(infile, zones):
    ttime = {}
    distance = {}
    cost = {}

    size = zones * zones

    # detects the number of intervals we have in the matrix
    if not Check_Tag(infile, "BINT"):
        return
    num_intervals = struct.unpack("i", infile.read(4))[0]
    intervals = list(struct.unpack(f"{num_intervals}i", infile.read(4 * num_intervals)))
    if not Check_Tag(infile, "EINT"):
        return

    def read_mat(interval, mat_name):
        if not Check_Tag(infile, "BMAT"):
            raise RuntimeError(f"check-tag-BMAT-failed for interval {interval} for matrix type {mat_name}")
        data = np.fromfile(infile, dtype="f", count=size)
        if data.size < size or not Check_Tag(infile, "EMAT"):
            raise RuntimeError(f"check-tag-EMAT-failed for interval {interval} for matrix type {mat_name}")
        return data.reshape(zones, zones)

    # for each interval, check tags and read in matrix
    for i, interval in enumerate(intervals):
        ttime[interval] = read_mat(interval, "auto_tt")
        distance[interval] = read_mat(interval, "auto_distance")
        cost[interval] = read_mat(interval, "auto_cost")

    return ttime, distance, cost
