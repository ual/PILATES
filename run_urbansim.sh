#!/bin/bash

set -eu

region_id=$1
year=$2
forecast_year=$3
time_step=$4
travel_model=$5
host_data=$6
sif_file=$7 # block_model_v2_pb.sif 
log_file=$6/$1_$2_$3_$5.txt
echo See log file $log_file

singularity exec \
    --cleanenv \
    --pwd /base/block_model_probaflow \
    -B $host_data:/base/block_model_probaflow/data/ \
    ${sif_file} \
    python -W ignore -u simulate.py -c -cf custom -l -sg -r $region_id -i $year -y $forecast_year -f $time_step -t $travel_model \
    > $log_file
