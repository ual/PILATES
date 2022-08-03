#!/bin/bash
#SBATCH --job-name=land_use
#SBATCH --account=POLARIS
#SBATCH --partition=bdwall
#SBATCH --nodes=1
#SBATCH --time=03:00:00
#SBATCH --mail-user=jauld@anl.gov
#SBATCH --mail-type=ALL

module restore polaris_mod

python run.py
