import struct

import numpy as np
import pandas as pd


def build_index(infile, zones: np.ndarray) -> pd.DataFrame:
    indices = struct.unpack(f"{zones * 2}i", infile.read(8 * zones))

    index = pd.DataFrame(np.reshape(indices, (-1, 2)), columns=["zones", "index"]).sort_values(by=["index"])
    if not index.zones.is_unique:
        print(f"Error, duplicate zone ids found: {index.zones.duplicated()}")
    return index
