import numpy as np
import openmatrix as omx


def export_hwy_omx(matrices, path_file, index, intervals):
    omx_export = omx.open_file(path_file, "w")

    # Export traffic
    for skm_name, skm_data in matrices.items():
        for interval, mat_data in skm_data.items():
            nm = f"auto_{interval}_{skm_name}"
            omx_export[nm] = mat_data
            omx_export[nm].attrs.timeperiod = str(interval)
            omx_export[nm].attrs.mode = "auto"
            omx_export[nm].attrs.metric = skm_name

    omx_export.root._v_attrs["interval_count"] = np.array([len(intervals)]).astype("int32")
    omx_export.root._v_attrs["update_intervals"] = np.array(intervals).astype("float32")
    zones = np.array(index.zones.values.astype(np.int32))
    omx_export.create_mapping("taz", zones)
    omx_export.close()
