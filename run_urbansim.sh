#!/bin/bash

host_data=$1
region_id=$2
year=$3
forecast_year=$4
time_step=$5

singularity exec \
    --cleanenv \
    --pwd /base/block_model_probaflow \
    -B $host_data:/base/block_model_probaflow/data/ \
    block_model_v2_pb.sif \
    python -u simulate.py -c -cf custom -l -sg -r $region_id -i $year -y $forecast_year -f $time_step
