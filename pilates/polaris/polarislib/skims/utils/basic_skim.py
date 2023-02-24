import importlib.util as iutil
import logging
from pathlib import Path

import numpy as np
import pandas as pd


INFINITE_TRANSIT = 1e6


class SkimBase:
    def __init__(self):
        self.version = "omx"
        self._inf = np.inf
        self.zone_id_to_index_map = {}
        self.zone_index_to_id_map = {}
        self.index = pd.DataFrame([])
        self.intervals = []

    @property
    def num_zones(self) -> int:
        return self.index.shape[0]

    def __setattr__(self, key, value):
        self.__dict__[key] = value

        if key != "index":
            return
        self.zone_id_to_index_map.clear()
        self.zone_index_to_id_map.clear()

        if value.shape[0]:
            self.zone_id_to_index_map = dict(zip(value.zones, value.index))
            self.zone_index_to_id_map = dict(zip(value.index, value.zones))


class SkimBaseBinary:
    def __init__(self):
        self.version = 3
        self._inf = np.inf
        self.zone_id_to_index_map = {}
        self.zone_index_to_id_map = {}
        self.index = pd.DataFrame([])
        self.intervals = []

    @property
    def num_zones(self) -> int:
        return self.index.shape[0]

    def __setattr__(self, key, value):
        self.__dict__[key] = value

        if not key == "index":
            return
        self.zone_id_to_index_map.clear()
        self.zone_index_to_id_map.clear()

        if value.shape[0]:
            self.zone_id_to_index_map = dict(zip(value.zones, value.index))
            self.zone_index_to_id_map = dict(zip(value.index, value.zones))


def upgrade_skim_to_omx(skim_class, folder: Path):
    omx_file, bin_file = [folder / f"{skim_class.prefix}_skim_file.{ext}" for ext in ["omx", "bin"]]
    if not bin_file.exists():
        logging.info(f"Skipping OMX conversion, {bin_file} missing")
        return
    if omx_file.exists():
        logging.info(f"Skipping OMX conversion, {omx_file} already exists")
        return
    logging.info(f"Converting {bin_file} -> {omx_file}")
    skim_class(bin_file).write(omx_file)
