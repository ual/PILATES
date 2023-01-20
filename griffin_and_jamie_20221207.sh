#!/bin/bash
#SBATCH --job-name=land_use
#SBATCH --account=TPS
#SBATCH --partition=TPS
#SBATCH --nodes=1
#SBATCH --time=50:00:00
#SBATCH --mail-user=jauld@anl.gov
#SBATCH --mail-type=ALL

set -eu

main() {
    cd /lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/PILATES-SRC-JA
    module_load
    setup_venv
    . venv/bin/activate
    python3 ./run.py "$@"
}

# /lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/no_cacc_ref/PILATES/pilates/urbansim/data/custom_mpo_48197301_model_data.h5
# /lcrc/project/POLARIS/bebop/SMART_FY22_LAND_USE/PILATES-SRC/pilates/urbansim/data/*.h5

setup_venv() {
    python3 -m pip install --user virtualenv
    [[ -d venv ]] || virtualenv venv
    . venv/bin/activate
    # python3 -m pip install -r requirements.txt
}

module_load() {

    # BEBOP
    # module load gcc/10.2.0-z53hda3
    # module load python/3.8.10-6kl7zkj
    # module load singularity/3.6.4

    # CROSSOVER
    module load gcc/9.2.0-sjjvpmg
    module load python/3.8.10-obsyt5i
    module load singularity/3.10.2

    python3 -m ensurepip --upgrade
}

main "$@"