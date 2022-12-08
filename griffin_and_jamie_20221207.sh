#!/usr/bin/env bash

set -eu

main() {
    echo "hi"
    # module_load
    # setup_venv
    . venv/bin/activate
    python3 ./run.py
}

# /lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/no_cacc_ref/PILATES/pilates/urbansim/data/custom_mpo_48197301_model_data.h5
# /lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/PILATES-SRC/pilates/urbansim/data/*.h5

setup_venv() {
    python3 -m pip install --user virtualenv
    [[ -d venv ]] || virtualenv venv
    . venv/bin/activate
    python3 -m pip install -r requirements.txt
}

module_load() {
    module load gcc/8.2.0-xhxgy33
    module load python/3.8.10-estdwmt
    python3 -m ensurepip --upgrade
}
main "$@"