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
export BAUS_INPUT_BUCKET_PATH=/output/$BAUS_INPUT_BUCKET
export BAUS_OUTPUT_BUCKET=${BAUS_OUTPUT_BUCKET:-urbansim-outputs}
export PILATES_BASE_OUTPUT_PATH=/output/$BAUS_OUTPUT_BUCKET
export BEAM_EXCHANGE_SCENARIO_FOLDER=$PILATES_BASE_OUTPUT_PATH

export ASYNTH_PATH=${ASYNTH_PATH:-~/projects/activitysynth}
export ASYNTH_DATA_PATH=$ASYNTH_PATH/activitysynth/data
export ASYNTH_DATA_OUTPUT_PATH=$ASYNTH_PATH/activitysynth/output
export ASYNTH_DATA_OUTPUT_FILE=${ASYNTH_DATA_OUTPUT_FILE:-model_data_output.h5}
export ASYNTH_DATA_OUTPUT_FILEPATH="$ASYNTH_DATA_OUTPUT_PATH/$ASYNTH_DATA_OUTPUT_FILE"

export START_YEAR=${1:?ARG \#1 "IN_YEAR" not specified}
export N_YEARS=${2:?ARG \#2 "N_YEARS" not specified}
export BEAM_BAUS_ITER_FREQ=${3:?ARG \#3 "BEAM_BAUS_ITER_FREQ" not specified}
export BAUS_ITER_FREQ=${4:?ARG \#4 "BAUS_ITER_FREQ" not specified}
export SCENARIO=${5:?ARG \#5 "SCENARIO" not specified}
export BEAM_CONFIG=${6:?ARG \#6 "BEAM_CONFIG" not specified}
export INITIAL_SKIMS_PATH=${7:?ARG \#7 "INITIAL_SKIMS_PATH" not specified}
export IN_YEAR_OUTPUT=${8:-off}

# to distinguish urbansim output for beam (which will be in /output/$BAUS_OUTPUT_BUCKET) 
# and results of whole pilates run (which will be in /output/$BAUS_OUTPUT_BUCKET/$SCENARIO)
export PILATES_BASE_OUTPUT_PATH=$PILATES_BASE_OUTPUT_PATH/$SCENARIO

#export SKIMS_FILEPATH=s3://$SKIMS_BUCKET/$SKIMS_FNAME

# send stdout and stderr to console and logs
#exec > >(tee $LOG_PATH/$(date +%d%B%Y_%H%M%S).log)
#exec 2>&1

((LAST_YEAR = START_YEAR + N_YEARS))

echo "START_YEAR: $START_YEAR"
echo "LAST_YEAR: $LAST_YEAR"
echo "BAUS_INPUT_BUCKET_PATH: $BAUS_INPUT_BUCKET_PATH"
echo "PILATES_BASE_OUTPUT_PATH: $PILATES_BASE_OUTPUT_PATH"

while ((START_YEAR < LAST_YEAR)); do
	echo "########### RUNNING BEAM FOR YEAR $START_YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	# running beam or using initial skims file path if this is first iteration
	if [[ ( -z "$INITIAL_SKIMS_PATH" ) || ($START_YEAR != ${1})  ]]; then
		echo "Running from config: $BEAM_CONFIG"
		export BEAM_OUTPUT=$PILATES_BASE_OUTPUT_PATH/$START_YEAR/beam
		echo "Beam env was set. $(env | grep '^BEAM_OUTPUT=')"
		cd /beam-project
		/beam/bin/beam --config $BEAM_CONFIG
		cd -

		# Find the most recent skims.csv.gz output in the output directory, we add timestamp in the find command to ensure this
		SKIMS_FILEPATH=$(find $BEAM_OUTPUT -name "*.skims.csv.gz" -printf "%T@ %Tc &%p\n"  | sort -r | head -n 1 | cut -d '&' -f 2)
		echo "Skim file from beam: $SKIMS_FILEPATH"
	else
		SKIMS_FILEPATH=$INITIAL_SKIMS_PATH
		echo "Initial skim file: $SKIMS_FILEPATH"
	fi

	
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	# What is the end year of the BAUS run
	((END_YEAR = START_YEAR + BEAM_BAUS_ITER_FREQ))

	# Different steps get run if simulating from base-year data
	if [[ $START_YEAR == 2010 ]]; then

		# Make in-year model data .h5 from base data
		echo "########### MAKING MODEL DATA HDF STORE FOR BAUS ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
			-m -b -i $BAUS_INPUT_BUCKET_PATH/base/base \
			-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

		# Run data pre-processing step
		echo "########### PRE-PROCESSING BAUS DATA ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $BAUS_PATH \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
			--mode preprocessing
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

		# Run bayarea_urbansim model estimation
		echo "########### RUNNING URBANSIM ESTIMATION ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $BAUS_PATH \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
			--mode estimation
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	else

		# Make in-year model data .h5 from intermediate year data
		echo "########### MAKING MODEL DATA HDF STORE FOR BAUS ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
			-m -i $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$START_YEAR \
			-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	fi


	# Run bayarea_urbansim simulation
	echo "########### RUNNING URBANSIM SIMULATION $START_YEAR to $END_YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"
	cd $BAUS_PATH \
		&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_5/bin/python baus.py -c -o \
		-y $START_YEAR,$END_YEAR -n $BAUS_ITER_FREQ --mode simulation
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"


	# generate in-year ouputs if specified
	if [[ $IN_YEAR_OUTPUT == "on" ]]; then

		# Write base year baus outputs to csv
		echo "########### PROCESSING ACTIVITYSYNTH DATA FOR IN-YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
			make_csvs_from_output_store.py -y $START_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
			-o $ASYNTH_DATA_PATH
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

		# Run activitysynth for base year
		echo "########### RUNNING ACTIVITYSYNTH FOR IN-YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $ASYNTH_PATH/activitysynth \
			&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

		# Write in-year activitysynth outputs to s3 as .csv and then
		# delete the output data from the default output data directory
		# so that orca doesn't just append to it next time around
		echo "########### SENDING IN-YEAR ACTIVITYSYNTH OUTPUTS TO S3 ########### $(date +"%Y-%m-%d_%H-%M-%S")"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
			make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
			-o $PILATES_BASE_OUTPUT_PATH/$START_YEAR/urbansim -x
		echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	fi

	# Write end year baus outputs to csv
	echo "########### PROCESSING ACTIVITYSYNTH DATA FOR END-YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"
	cd $PILATES_PATH/scripts \
		&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
		make_csvs_from_output_store.py -y $END_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
		-o $ASYNTH_DATA_PATH
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"


	# Run activitysynth for end-year
	echo "########### RUNNING ACTIVITYSYNTH FOR END-YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"
	cd $ASYNTH_PATH/activitysynth \
		&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"


	# Write out-year activitysynth outputs to s3 as .csv
	echo "########### SENDING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 OUTPUT BUCKET########### $(date +"%Y-%m-%d_%H-%M-%S")"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $PILATES_BASE_OUTPUT_PATH/$END_YEAR/urbansim
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	echo "########### COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 INPUT BUCKET########### $(date +"%Y-%m-%d_%H-%M-%S")"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$END_YEAR
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	# Write out-year activitysynth outputs to $BEAM_EXCHANGE_SCENARIO_FOLDER folder
	# The same folder must be used as for beam param `beam.exchange.scenario.folder`
	# Now it looks like `beam.exchange.scenario.folder="/output/urbansim-outputs"`
	echo "########### COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO OUTPUT BUCKET TO BE BEAM INPUT ########### $(date +"%Y-%m-%d_%H-%M-%S")"
	echo "copying to $BEAM_EXCHANGE_SCENARIO_FOLDER"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $BEAM_EXCHANGE_SCENARIO_FOLDER/
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	((START_YEAR = $START_YEAR + BEAM_BAUS_ITER_FREQ))
done

echo "########### RUNNING BEAM FOR YEAR $START_YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"

echo "Running from config: $BEAM_CONFIG"
export BEAM_OUTPUT=$PILATES_BASE_OUTPUT_PATH/$START_YEAR/beam
echo "Beam env was set. $(env | grep '^BEAM_OUTPUT=')"
cd /beam-project
/beam/bin/beam --config $BEAM_CONFIG
cd -

#       ################       ##############         #######        ################
#             ####             ###                   ###    ###            ####
#             ####             ###                   ###                   ####
#             ####             ###                     ####                ####
#             ####             #########                 ####              ####
#             ####             ###                         ###             ####
#             ####             ###                          ###            ####
#             ####             ###                  ###    ###             ####
#             ####             ##############         #######              ####

echo "Uploading complete output from local path: $PILATES_BASE_OUTPUT_PATH"
aws --region us-east-2 s3 cp $PILATES_BASE_OUTPUT_PATH s3://inm-test-run-pilates/pilates-outputs/"$SCENARIO"_"$(date +"%Y-%m-%d_%H-%M-%S")" --recursive

echo "########### ALL DONE!!! ########### $(date +"%Y-%m-%d_%H-%M-%S")"
