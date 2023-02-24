import struct

import numpy as np
import pandas as pd

from ..utils.build_matrix_index import build_index
from ..utils.check_tag import Check_Tag


def read_transit_index(infile):
    if not Check_Tag(infile, "BZON"):
        infile.seek(-4, 1)
        # get zones
        zones = struct.unpack("i", infile.read(4))[0]
        mat_index = pd.DataFrame({"zones": 1 + np.arange(zones), "index": np.arange(zones)})
    # if zone tag is there, read in numzones, then the zone id-index pairs
    else:
        zones = struct.unpack("i", infile.read(4))[0]
        mat_index = build_index(infile, zones)
        # tag = struct.unpack("<4s",infile.read(4))[0]
        if not Check_Tag(infile, "EZON"):
            raise ValueError("Could not build an index")
    return mat_index
