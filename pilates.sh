#!/bin/bash

# exit when any command fails
set -e

# define default values for env vars if not set
export CONDA_DIR=${CONDA_DIR:-~/anaconda3}
export CONDA_ENV_BAUS_ORCA_1_4=${CONDA_ENV_BAUS_ORCA_1_4:-baus}
export CONDA_ENV_BAUS_ORCA_1_5=${CONDA_ENV_BAUS_ORCA_1_5:-baus_output}
export CONDA_ENV_ASYNTH=${CONDA_ENV_ASYNTH:-activitysynth}

export PILATES_PATH=${PILATES_PATH:-~/projects/PILATES}
export LOG_PATH=$PILATES_PATH/logs

export BAUS_PATH=${BAUS_PATH:-~/projects/bayarea_urbansim}
export BAUS_DATA_STORE_PATH=$BAUS_PATH/data
export BAUS_DATA_OUTPUT_PATH=$BAUS_PATH/output
export BAUS_DATA_OUTPUT_FILE=${BAUS_DATA_OUTPUT_FILE:-model_data_output.h5}
export BAUS_DATA_OUTPUT_FILEPATH=$BAUS_DATA_OUTPUT_PATH/$BAUS_DATA_OUTPUT_FILE
export BAUS_INPUT_BUCKET=${BAUS_INPUT_BUCKET:-urbansim-inputs}
export BAUS_INPUT_BUCKET_PATH=s3://$BAUS_INPUT_BUCKET
export BAUS_OUTPUT_BUCKET=${BAU_OUTPUT_BUCKET:-urbansim-outputs}
export BAUS_OUTPUT_BUCKET_PATH=s3://$BAUS_OUTPUT_BUCKET


export SKIMS_BUCKET=${SKIMS_BUCKET:-urbansim-beam}

export ASYNTH_PATH=${ASYNTH_PATH:-~/projects/activitysynth}
export ASYNTH_DATA_PATH=$ASYNTH_PATH/activitysynth/data
export ASYNTH_DATA_OUTPUT_PATH=$ASYNTH_PATH/activitysynth/output
export ASYNTH_DATA_OUTPUT_FILE=${ASYNTH_DATA_OUTPUT_FILE:-model_data_output.h5}
export ASYNTH_DATA_OUTPUT_FILEPATH="$ASYNTH_DATA_OUTPUT_PATH/$ASYNTH_DATA_OUTPUT_FILE"

# define default values/behavior for command-line arguments
export IN_YEAR=${1:?ARG \#1 "IN_YEAR" \#1 not specified}
export OUT_YEAR=${2:?ARG \#2 "OUT_YEAR" not specified}
export BAUS_ITER_FREQ=${3:?ARG \#3 "BAUS_ITER_FREQ" not specified}
export SCENARIO=${4:?ARG \#4 "SCENARIO" not specified}
export SKIMS_FNAME=${5:?ARG \#5 "SKIMS_FNAME" not specified}
export IN_YEAR_OUTPUT=${6:-off}

export SKIMS_FILEPATH=s3://$SKIMS_BUCKET/$SKIMS_FNAME


# send stdout and stderr to console and logs
exec > >(tee $LOG_PATH/$(date +%d%B%Y_%H%M%S).log)
exec 2>&1


# 2010 data can be base input data as well so we call it
# base data when its base data even though the year is 2010
if [[ $IN_YEAR == 2010 ]]; then
	export BAUS_INPUT_DATA_YEAR="base"
else
	export BAUS_INPUT_DATA_YEAR=$IN_YEAR
fi


# Make in-year model data .h5
cd $PILATES_PATH/scripts \
&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
-n -b -i $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$BAUS_INPUT_DATA_YEAR \
-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH


# Run data pre-processing step
cd $BAUS_PATH \
&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
--mode preprocessing


# Run bayarea_urbansim model estimation
cd $BAUS_PATH \
&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
--mode estimation


# Run bayarea_urbansim simulation 
cd $BAUS_PATH \
&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_5/bin/python baus.py -c -o \
-y $IN_YEAR,$OUT_YEAR -n $BAUS_ITER_FREQ --mode simulation


# generate in-year ouputs if specified
if [[ $IN_YEAR_OUTPUT == "on" ]]; then

	# Write base year baus outputs to csv
	cd $PILATES_PATH/scripts \
	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
	make_csvs_from_output_store.py -y $IN_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
	-o $ASYNTH_DATA_PATH

	# Run activitysynth for base year
	cd $ASYNTH_PATH/activitysynth \
	&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o

	# Write in-year activitysynth outputs to s3 as .csv and then 
	# delete the output data from the default output data directory
	# so that orca doesn't just append to it next time around
	cd $PILATES_PATH/scripts \
	&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
	make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
	-o $BAUS_OUTPUT_BUCKET_PATH/$(date +%d%B%Y)/$SCENARIO/$IN_YEAR -x

	echo "Wrote ActivitySynth outputs for "
fi


# Write end year baus outputs to csv
cd $PILATES_PATH/scripts \
&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
make_csvs_from_output_store.py -y $OUT_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
-o $ASYNTH_DATA_PATH


# Run activitysynth for end-year
cd $ASYNTH_PATH/activitysynth \
&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o


# Write out-year activitysynth outputs to s3 as .csv
cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
-o $BAUS_OUTPUT_BUCKET_PATH/$(date +%d%B%Y)/$SCENARIO/$OUT_YEAR


