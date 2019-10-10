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
export PILATES_OUTPUT_PATH=/output/$BAUS_OUTPUT_BUCKET
export BEAM_EXCHANGE_SCENARIO_FOLDER=$PILATES_OUTPUT_PATH

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
export OUTPUT_BUCKET_BASE_PATH=${7:?ARG \#7 "OUTPUT_BUCKET_BASE_PATH" not specified}
export IN_YEAR_OUTPUT=${8:-off}
export INITIAL_SKIMS_PATH=${9:-""}

scenarioWithDate="$SCENARIO"_"$(date +"%Y-%m-%d_%H-%M-%S")"
export PILATES_OUTPUT_PATH=$PILATES_OUTPUT_PATH/$scenarioWithDate
export S3_BUCKET_PATH=s3:$OUTPUT_BUCKET_BASE_PATH/$scenarioWithDate

((LAST_YEAR = START_YEAR + N_YEARS))

echo "START_YEAR: $START_YEAR"
echo "LAST_YEAR: $LAST_YEAR"
echo "BAUS_INPUT_BUCKET_PATH: $BAUS_INPUT_BUCKET_PATH"
echo "PILATES_OUTPUT_PATH: $PILATES_OUTPUT_PATH"
echo "S3_BUCKET_PATH: $S3_BUCKET_PATH"

echoMilestone(){
  mnumber=${1:-''}
  mtext=${2:-DONE}
  echo "MILESTONE #$mnumber $(date +"%Y-%m-%d_%H-%M-%S")  $mtext  ###########"
}

uploadDirectoryToS3(){
  echoMilestone "uploadToS3" "COPYING OUTPUTS TO S3 BUCKET from $1 to $2"
  aws --region us-east-2 s3 cp $1 $2 --recursive
  echoMilestone "uploadToS3"
}

while ((START_YEAR < LAST_YEAR)); do

	# running beam or using initial skims file path if this is first iteration
	if [[ ( -z "$INITIAL_SKIMS_PATH" ) || ($START_YEAR != ${1})  ]]; then
		echoMilestone 1 "RUNNING BEAM FOR YEAR $START_YEAR with config: $BEAM_CONFIG"
		export BEAM_OUTPUT=$PILATES_OUTPUT_PATH/$START_YEAR/beam
		cd /beam-project
		/beam/bin/beam --config $BEAM_CONFIG
		cd -

		uploadDirectoryToS3 "$BEAM_OUTPUT" "$S3_BUCKET_PATH/$START_YEAR/beam" &

		# Find the most recent skims.csv.gz output in the output directory, we add timestamp in the find command to ensure this
		SKIMS_FILEPATH=$(find $BEAM_OUTPUT -name "*.skims.csv.gz" -printf "%T@ %Tc &%p\n"  | sort -r | head -n 1 | cut -d '&' -f 2)
		echo "Skim file from beam: $SKIMS_FILEPATH"
	else
		echoMilestone 1 "skipping beam for year $START_YEAR"
		SKIMS_FILEPATH=s3:$INITIAL_SKIMS_PATH
		echo "Initial skim file:$SKIMS_FILEPATH"
	fi

	echoMilestone 1

	# What is the end year of the BAUS run
	((END_YEAR = START_YEAR + BEAM_BAUS_ITER_FREQ))

	# Different steps get run if simulating from base-year data
	if [[ $START_YEAR == 2010 ]]; then

		# Make in-year model data .h5 from base data
		echoMilestone "2.1" "MAKING MODEL DATA HDF STORE FOR BAUS"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
			-m -b -i $BAUS_INPUT_BUCKET_PATH/base/base \
			-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH \
			&& echoMilestone "2.1"

		# Run data pre-processing step
		echoMilestone "2.2" "PRE-PROCESSING BAUS DATA"
		cd $BAUS_PATH \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
			--mode preprocessing \
			&& echoMilestone "2.2"

		# Run bayarea_urbansim model estimation
		echoMilestone "2.3" "RUNNING URBANSIM ESTIMATION"
		cd $BAUS_PATH \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python baus.py -c \
			--mode estimation \
			&& echoMilestone "2.3"

	else

		# Make in-year model data .h5 from intermediate year data
		echoMilestone 2 "MAKING MODEL DATA HDF STORE FOR BAUS)"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
			-m -i $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$START_YEAR \
			-s $SKIMS_FILEPATH -o $BAUS_DATA_STORE_PATH \
			&& echoMilestone 2

	fi


	# Run bayarea_urbansim simulation
	echoMilestone 3 "RUNNING URBANSIM SIMULATION $START_YEAR to $END_YEAR"
	cd $BAUS_PATH \
		&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_5/bin/python baus.py -c -o \
		-y $START_YEAR,$END_YEAR -n $BAUS_ITER_FREQ --mode simulation \
		&& echoMilestone 3


	# generate in-year ouputs if specified
	if [[ $IN_YEAR_OUTPUT == "on" ]]; then

		# Write base year baus outputs to csv
		echoMilestone "4.1" "PROCESSING ACTIVITYSYNTH DATA FOR IN-YEAR"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
			make_csvs_from_output_store.py -y $START_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
			-o $ASYNTH_DATA_PATH \
			&& echoMilestone "4.1"

		# Run activitysynth for base year
		echoMilestone "4.2" "RUNNING ACTIVITYSYNTH FOR IN-YEAR"
		cd $ASYNTH_PATH/activitysynth \
			&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o \
			&& echoMilestone "4.2"

		# Write in-year activitysynth outputs to s3 as .csv and then
		# delete the output data from the default output data directory
		# so that orca doesn't just append to it next time around
		echoMilestone "4.3" "SENDING IN-YEAR ACTIVITYSYNTH OUTPUTS TO S3"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
			make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
			-o $PILATES_OUTPUT_PATH/$START_YEAR/urbansim -x \
			&& echoMilestone "4.3"

    uploadDirectoryToS3 "$PILATES_OUTPUT_PATH/$START_YEAR/urbansim" "$S3_BUCKET_PATH/$START_YEAR/urbansim" &

	fi

	# Write end year baus outputs to csv
	echoMilestone 5 "PROCESSING ACTIVITYSYNTH DATA FOR END-YEAR"
	cd $PILATES_PATH/scripts \
		&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python \
		make_csvs_from_output_store.py -y $END_YEAR -d $BAUS_DATA_OUTPUT_FILEPATH \
		-o $ASYNTH_DATA_PATH \
		&& echoMilestone 5


	# Run activitysynth for end-year
	echoMilestone 6 "RUNNING ACTIVITYSYNTH FOR END-YEAR"
	cd $ASYNTH_PATH/activitysynth \
		&& $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python run.py -o \
		&& echoMilestone 6


	# Write out-year activitysynth outputs to s3 as .csv
	echoMilestone 7 "SENDING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 OUTPUT BUCKET"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $PILATES_OUTPUT_PATH/$END_YEAR/urbansim \
		&& echoMilestone 7

  uploadDirectoryToS3 "$PILATES_OUTPUT_PATH/$END_YEAR/urbansim" "$S3_BUCKET_PATH/$END_YEAR/urbansim" &

	echoMilestone 8 "COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO S3 INPUT BUCKET"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $BAUS_INPUT_BUCKET_PATH/$SCENARIO/$END_YEAR \
		&& echoMilestone 8

	# Write out-year activitysynth outputs to $BEAM_EXCHANGE_SCENARIO_FOLDER folder
	# The same folder must be used as for beam param `beam.exchange.scenario.folder`
	# Now it looks like `beam.exchange.scenario.folder="/output/urbansim-outputs"`
	echoMilestone 9 "COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO OUTPUT BUCKET TO BE BEAM INPUT. to $BEAM_EXCHANGE_SCENARIO_FOLDER"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $BEAM_EXCHANGE_SCENARIO_FOLDER/ \
		&& echoMilestone 9

	((START_YEAR = $START_YEAR + BEAM_BAUS_ITER_FREQ))

done

echoMilestone 10 "RUNNING BEAM FOR YEAR $START_YEAR with config: $BEAM_CONFIG"
export BEAM_OUTPUT=$PILATES_OUTPUT_PATH/$START_YEAR/beam
cd /beam-project
/beam/bin/beam --config $BEAM_CONFIG
cd -
echoMilestone 10

uploadDirectoryToS3 "$BEAM_OUTPUT" "$S3_BUCKET_PATH/$START_YEAR/beam" &

# waiting for all background jobs to be finished
wait

echoMilestone
