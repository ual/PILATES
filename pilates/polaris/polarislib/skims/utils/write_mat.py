import numpy as np

from .write_tag import write_tag


def write_mat(infile, mat_data: np.ndarray):
    write_tag(infile, "BMAT")
    np.array(mat_data.flatten(), np.float32).tofile(infile)
    write_tag(infile, "EMAT")
