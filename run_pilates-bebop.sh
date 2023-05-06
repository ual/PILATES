#!/bin/bash
#SBATCH --job-name=land_use
#SBATCH --account=POLARIS
#SBATCH --partition=bdwall
#SBATCH --nodes=1
#SBATCH --time=30:00:00
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

setup_venv() {
    python3 -m pip install --user virtualenv
    [[ -d venv ]] || virtualenv venv
    . venv/bin/activate
    # python3 -m pip install -r requirements.txt
}

module_load() {

    # BEBOP
    module load gcc/10.2.0-z53hda3
    module load python/3.8.10-6kl7zkj
    module load singularity/3.6.4

    python3 -m ensurepip --upgrade
}

main "$@"