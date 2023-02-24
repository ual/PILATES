import struct

import numpy as np
import pandas as pd

from ..utils.build_matrix_index import build_index
from ..utils.check_tag import Check_Tag


def read_highway_index(infile):
    if Check_Tag(infile, "MODE", rewind=True):
        _ = infile.read(4 + 4)  # 4 for "MODE" and 4-bytes for the mode id

    # check to see if <BZON> tag is in the file
    if Check_Tag(infile, "BZON"):
        zones = struct.unpack("i", infile.read(4))[0]
        mat_index = build_index(infile, zones)
        if not Check_Tag(infile, "EZON"):
            raise RuntimeError("Missing EZON Tag")
    else:
        infile.seek(-4, 1)
        modes, zones = struct.unpack("ii", infile.read(8))
        mat_index = pd.DataFrame({"zones": 1 + np.arange(zones), "index": np.arange(zones)})

    return zones, mat_index
