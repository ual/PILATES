#!/bin/bash
#SBATCH --job-name=land_use --account=TPS --partition=TPS
#SBATCH --nodes=1 --ntasks-per-node=64 --exclusive
#SBATCH --time=11:00:00
#SBATCH --mail-user=jauld@anl.gov
#SBATCH --mail-type=ALL

set -eu

main() {
    cd /lcrc/project/POLARIS/crossover/PILATES
    module_load
    setup_venv
    python3 ./run.py "$@"
}

setup_venv() {
    set +u; eval "$(conda shell.bash hook)"; set -u
    conda activate pilates
    # python3 -m pip install -r requirements.txt
}

module_load() {
    module load gcc/10.4.0-ckeolqi
    module load anaconda3
    module load singularity/3.10.2
    module load hdf5/1.12.1-l4cjxhb
}

main "$@"


