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
export BAUS_OUTPUT_BUCKET_PATH=/output/$BAUS_OUTPUT_BUCKET

#export SKIMS_BUCKET=${SKIMS_BUCKET:-urbansim-beam}

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
export SKIP_FIRST_BEAM=${7:-off}
export IN_YEAR_OUTPUT=${8:-off}

#export SKIMS_FILEPATH=s3://$SKIMS_BUCKET/$SKIMS_FNAME

# send stdout and stderr to console and logs
#exec > >(tee $LOG_PATH/$(date +%d%B%Y_%H%M%S).log)
#exec 2>&1

((LAST_YEAR = START_YEAR + N_YEARS))

echo "START_YEAR: $START_YEAR"
echo "LAST_YEAR: $LAST_YEAR"
echo "BAUS_INPUT_BUCKET_PATH: $BAUS_INPUT_BUCKET_PATH"
echo "BAUS_OUTPUT_BUCKET_PATH: $BAUS_OUTPUT_BUCKET_PATH"

while ((START_YEAR < LAST_YEAR)); do
	echo "########### RUNNING BEAM FOR YEAR $START_YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	#running beam under root so that we can map outputs
	if [[ ($SKIP_FIRST_BEAM == "off") || ($START_YEAR != ${1})  ]]; then
		echo "Running from config: $BEAM_CONFIG" 
		cd /beam-project
		/beam/bin/beam --config $BEAM_CONFIG
		cd -
	fi

	# Fine the most recent skims.csv.gz output in the output directory, we add timestamp in the find command to ensure this
	SKIMS_FILEPATH=$(find /beam-project/output/sfbay -name "*.skims.csv.gz" -printf "%T@ %Tc &%p\n"  | sort -r | head -n 1 | cut -d '&' -f 2)
	echo "Skim file $SKIMS_FILEPATH"
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
			-o $BAUS_OUTPUT_BUCKET_PATH/$(date +%d%B%Y)/$SCENARIO/$START_YEAR -x
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
		-o $BAUS_OUTPUT_BUCKET_PATH/$(date +%d%B%Y)/$SCENARIO/$END_YEAR
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	echo "########### COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 INPUT BUCKET########### $(date +"%Y-%m-%d_%H-%M-%S")"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$END_YEAR
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	# Write out-year activitysynth outputs to $BAUS_OUTPUT_BUCKET_PATH folder
	# The same folder must be used as for beam param `beam.exchange.scenario.folder`
	# Now it looks like `beam.exchange.scenario.folder="/output/urbansim-outputs"`
	echo "########### COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO OUTPUT BUCKET TO BE BEAM INPUT ########### $(date +"%Y-%m-%d_%H-%M-%S")"
	echo "copying to $BAUS_OUTPUT_BUCKET_PATH"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $BAUS_OUTPUT_BUCKET_PATH/
	echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

	((START_YEAR = $START_YEAR + BEAM_BAUS_ITER_FREQ))
done

echo "########### RUNNING BEAM FOR YEAR $START_YEAR ########### $(date +"%Y-%m-%d_%H-%M-%S")"

echo "Running from config: $BEAM_CONFIG" 
cd /beam-project
/beam/bin/beam --config $BEAM_CONFIG
cd -

# COPY ALL OUTPUTS TO S3
RUN_DATE=$(date +"%Y-%m-%d_%H-%M-%S")
#TO_COPY=$(find /beam-project/output/sfbay -mindepth 1 -maxdepth 1 -type d -printf "%T@ %Tc &%p\n"  | sort -r | cut -d '&' -f 2)
TO_COPY='/beam-project/output/sfbay'
echo "Uploading BEAM output from local path: $TO_COPY"
aws --region us-east-2 s3 cp $TO_COPY s3://pilates-outputs/"$SCENARIO"_"$RUN_DATE"/beam --recursive
#cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
	#      upload_last_beam_output.py -o $TO_COPY -b pilates-outputs -s ${SCENARIO}_${RUN_DATE}/beam

#((LAST_START_YEAR = $START_YEAR - BEAM_BAUS_ITER_FREQ))

echo "Uploading BAUS output from local path: $BAUS_OUTPUT_BUCKET_PATH"
aws --region us-east-2 s3 cp $BAUS_OUTPUT_BUCKET_PATH s3://pilates-outputs/"$SCENARIO"_"$RUN_DATE"/urbansim --recursive
#cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
	#      upload_last_beam_output.py -o $BAUS_OUTPUT_BUCKET_PATH -b pilates-outputs -s ${SCENARIO}_${RUN_DATE}/urbansim

echo "########### DONE! ########### $(date +"%Y-%m-%d_%H-%M-%S")"

echo "########### ALL DONE!!! ########### $(date +"%Y-%m-%d_%H-%M-%S")"
