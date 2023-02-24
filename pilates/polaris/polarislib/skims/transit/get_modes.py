import struct
import logging

logger = logging.getLogger(__name__)

#from polarislib.network.starts_logging import logger
from ..utils.check_tag import Check_Tag


def check_number_of_transit_modes(infile, modes):
    # check for the new MODES count tag.
    if not Check_Tag(infile, "MODE"):
        infile.seek(-4, 1)
    else:
        tmodes = struct.unpack("i", infile.read(4))[0]
        if tmodes != len(modes) + 1:
            logger.critical("We found fewer modes than expected")
