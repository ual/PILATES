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
export BAUS_OUTPUT_BUCKET=${BAUS_OUTPUT_BUCKET:-urbansim-outputs}
export BAUS_OUTPUT_BUCKET_PATH=s3://$BAUS_OUTPUT_BUCKET

export SKIMS_BUCKET=${SKIMS_BUCKET:-urbansim-beam}

export ASYNTH_PATH=${ASYNTH_PATH:-~/projects/activitysynth}
export ASYNTH_DATA_PATH=$ASYNTH_PATH/activitysynth/data
export ASYNTH_DATA_OUTPUT_PATH=$ASYNTH_PATH/activitysynth/output
export ASYNTH_DATA_OUTPUT_FILE=${ASYNTH_DATA_OUTPUT_FILE:-model_data_output.h5}
export ASYNTH_DATA_OUTPUT_FILEPATH="$ASYNTH_DATA_OUTPUT_PATH/$ASYNTH_DATA_OUTPUT_FILE"

# define default values/behavior for command-line arguments

export START_YEAR=${1:?ARG \#1 "IN_YEAR" not specified}
export N_YEARS=${2:?ARG \#2 "N_YEARS" not specified}
export BEAM_BAUS_ITER_FREQ=${3:?ARG \#3 "BEAM_BAUS_ITER_FREQ" not specified}
export BAUS_ITER_FREQ=${4:?ARG \#4 "BAUS_ITER_FREQ" not specified}
export SCENARIO=${5:?ARG \#5 "SCENARIO" not specified}
export BEAM_CONFIG=${6:?ARG \#6 "BEAM_CONFIG" not specified}
export IN_YEAR_OUTPUT=${7:-off}

#export SKIMS_FILEPATH=s3://$SKIMS_BUCKET/$SKIMS_FNAME

# send stdout and stderr to console and logs
#exec > >(tee $LOG_PATH/$(date +%d%B%Y_%H%M%S).log)
#exec 2>&1

((LAST_YEAR = START_YEAR + N_YEARS))

while ((START_YEAR < LAST_YEAR)); do
	echo "########### RUNNING BEAM FOR YEAR $START_YEAR ###########"

	/beam/bin/beam --config $BEAM_CONFIG

	# COPY SKIMS TO S3

   	echo "########### DONE! ###########"

    # What is the end year of the BAUS run
    ((END_YEAR = START_YEAR + BEAM_BAUS_ITER_FREQ))

    # Different steps get run if simulating from base-year data
    if [[ $START_YEAR == 2010 ]]; then

        # Make in-year model data .h5 from base data
        echo "########### MAKING MODEL DATA HDF STORE FOR BAUS ###########"
    #	cd $PILATES_PATH/scripts \
    #	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
    #	-m -b -i $BAUS_INPUT_BUCKET_PATH/base/base \
    #	-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH
        echo "########### DONE! ###########"

        # Run data pre-processing step
        echo "########### PRE-PROCESSING BAUS DATA ###########"
    #	cd $BAUS_PATH \
    #	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
    #	--mode preprocessing
        echo "########### DONE! ###########"

        # Run bayarea_urbansim model estimation
        echo "########### RUNNING URBANSIM ESTIMATION ###########"
    #	cd $BAUS_PATH \
    #	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
    #	--mode estimation
        echo "########### DONE! ###########"

    else

        # Make in-year model data .h5 from intermediate year data
        echo "########### MAKING MODEL DATA HDF STORE FOR BAUS ###########"
    #	cd $PILATES_PATH/scripts \
    #	&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
    #	-m -i $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$START_YEAR \
    #	-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH
        echo "########### DONE! ###########"

    fi


    # Run bayarea_urbansim simulation
    echo "########### RUNNING URBANSIM SIMULATION $START_YEAR to $END_YEAR ###########"
    #cd $BAUS_PATH \
    #&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_5/bin/python baus.py -c -o \
    #-y $START_YEAR,$END_YEAR -n $BAUS_ITER_FREQ --mode simulation
    echo "########### DONE! ###########"


    # generate in-year ouputs if specified
    if [[ $IN_YEAR_OUTPUT == "on" ]]; then

        # Write base year baus outputs to csv
        echo "########### PROCESSING ACTIVITYSYNTH DATA FOR IN-YEAR ###########"
        cd $PILATES_PATH/scripts \
        && $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
        make_csvs_from_output_store.py -y $START_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
        -o $ASYNTH_DATA_PATH
        echo "########### DONE! ###########"

        # Run activitysynth for base year
        echo "########### RUNNING ACTIVITYSYNTH FOR IN-YEAR ###########"
        cd $ASYNTH_PATH/activitysynth \
        && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o
        echo "########### DONE! ###########"

        # Write in-year activitysynth outputs to s3 as .csv and then
        # delete the output data from the default output data directory
        # so that orca doesn't just append to it next time around
        echo "########### SENDING IN-YEAR ACTIVITYSYNTH OUTPUTS TO S3 ###########"
        cd $PILATES_PATH/scripts \
        && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
        make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
        -o $BAUS_OUTPUT_BUCKET_PATH/$(date +%d%B%Y)/$SCENARIO/$START_YEAR -x
        echo "########### DONE! ###########"

    fi


    # Write end year baus outputs to csv
    echo "########### PROCESSING ACTIVITYSYNTH DATA FOR END-YEAR ###########"
    #cd $PILATES_PATH/scripts \
    #&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
    #make_csvs_from_output_store.py -y $END_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
    #-o $ASYNTH_DATA_PATH
    echo "########### DONE! ###########"


    # Run activitysynth for end-year
    echo "########### RUNNING ACTIVITYSYNTH FOR END-YEAR ###########"
    #cd $ASYNTH_PATH/activitysynth \
    #&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o
    echo "########### DONE! ###########"


    # Write out-year activitysynth outputs to s3 as .csv
    echo "########### SENDING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 OUTPUT BUCKET###########"
    #cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
    #make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
    #-o $BAUS_OUTPUT_BUCKET_PATH/$(date +%d%B%Y)/$SCENARIO/$END_YEAR
    echo "########### DONE! ###########"

    echo "########### COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 INPUT BUCKET###########"
    #cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
    #make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
    #-o $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$END_YEAR
    echo "########### DONE! ###########"

    ((START_YEAR = $START_YEAR + BEAM_BAUS_ITER_FREQ))
done

echo "########### RUNNING BEAM FOR YEAR $START_YEAR ###########"

    # RUN BEAM HERE

	# COPY ALL OUTPUTS TO S3

echo "########### DONE! ###########"

echo "########### ALL DONE!!! ###########"
