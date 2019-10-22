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

export ASYNTH_PATH=${ASYNTH_PATH:-~/projects/activitysynth}
export ASYNTH_DATA_PATH=$ASYNTH_PATH/activitysynth/data
export ASYNTH_DATA_OUTPUT_PATH=$ASYNTH_PATH/activitysynth/output
export ASYNTH_DATA_OUTPUT_FILE=${ASYNTH_DATA_OUTPUT_FILE:-model_data_output.h5}
export ASYNTH_DATA_OUTPUT_FILEPATH="$ASYNTH_DATA_OUTPUT_PATH/$ASYNTH_DATA_OUTPUT_FILE"

export START_YEAR=${1:?ARG \#1 "IN_YEAR" not specified}
export N_YEARS=${2:?ARG \#2 "N_YEARS" not specified}
export BEAM_BAUS_ITER_FREQ=${3:?ARG \#3 "BEAM_BAUS_ITER_FREQ" not specified}
export BAUS_ITER_FREQ=${4:?ARG \#4 "BAUS_ITER_FREQ" not specified}
export OUTPUT_FOLDER=${5:?ARG \#5 "OUTPUT_FOLDER" not specified}
export BEAM_CONFIG=${6:?ARG \#6 "BEAM_CONFIG" not specified}
export S3_OUTPUT_PATH=${7:?ARG \#7 "S3_OUTPUT_PATH" not specified}
export S3_DATA_REGION=${8:?ARG \#8 "S3_DATA_REGION" not specified}
export INPUT_DATA_PATH=${9:?ARG \#9 "INPUT_DATA_PATH" not specified}
export OUTPUT_DATA_PATH=${10:?ARG \#10 "OUTPUT_DATA_PATH" not specified}
export IN_YEAR_OUTPUT=${11:-off}
export INITIAL_SKIMS_PATH=${12:-""}

export BEAM_INITIAL_URBANSIM_DATA=$OUTPUT_DATA_PATH/initial
export BAUS_INITIAL_DATA=$INPUT_DATA_PATH/initial

export OUTPUT_DATA_PATH=$OUTPUT_DATA_PATH/$OUTPUT_FOLDER
export S3_OUTPUT_URL=s3:$S3_OUTPUT_PATH

((LAST_YEAR = START_YEAR + N_YEARS))

echo "START_YEAR: $START_YEAR"
echo "LAST_YEAR: $LAST_YEAR"
echo "INPUT_DATA_PATH: $INPUT_DATA_PATH"
echo "OUTPUT_DATA_PATH: $OUTPUT_DATA_PATH"
echo "S3_OUTPUT_URL: $S3_OUTPUT_URL"

echoMilestone(){
  mnumber=${1:-''}
  mtext=${2:-DONE}
  echo "MILESTONE $(date +"%Y-%m-%d %H:%M:%S") ########### #$mnumber $mtext"
}

uploadDirectoryToS3(){
  echoMilestone "uploadToS3" "COPYING OUTPUTS from local path $1 to s3 bucket $S3_DATA_REGION : $2"
  aws --region $S3_DATA_REGION s3 cp $1 $2 --recursive
  echoMilestone "uploadToS3"
}

prepareBeamConfig(){
  originalConfig=${1:?prepareBeamConfig: ARG \#1 beam config not specified}
  beamOutput=${2:?prepareBeamConfig: ARG \#2 beam output path not specified}
  pilatesData=${3:?prepareBeamConfig: ARG \#3 pilates data path not specified}
  currentYear=${4:?prepareBeamConfig: ARG \#4 current year not specified}

  beamConfigPath=$(dirname $originalConfig)
  beamOriginalConfigFile=$(basename $originalConfig)

  beamOriginalConfigFileName=${beamOriginalConfigFile%%.*}
  beamOriginalConfigFileExt=${beamOriginalConfigFile#*.}

  generatedConfigFile=$beamConfigPath/$beamOriginalConfigFileName.pilatesEntryPoint.$currentYear.$beamOriginalConfigFileExt

  # writing multiple lines to a single file
  cat >> $generatedConfigFile << EOL
# this file was generated by PILATES as entry point for BEAM

include "$beamOriginalConfigFile"

beam.outputs.baseOutputDirectory = "$beamOutput"
beam.exchange.scenario.folder = "$pilatesData"

EOL

  echo $generatedConfigFile
}

# if we do not skip initial beam run then copy urbansim data for first beam run into corresponding directory
if [[ -z "$INITIAL_SKIMS_PATH" ]]; then
  echoMilestone 0 "copy initial urbansim data for first beam run"
  mkdir -p $OUTPUT_DATA_PATH/$START_YEAR/urbansim
  cp -r $BEAM_INITIAL_URBANSIM_DATA/* $OUTPUT_DATA_PATH/$START_YEAR/urbansim
  echoMilestone 0
fi

# to copy initial urbansim data if any
# or, at least, to create output folder in s3 bucket with run-params file
uploadDirectoryToS3 "$OUTPUT_DATA_PATH" "$S3_OUTPUT_URL" &

while ((START_YEAR < LAST_YEAR)); do

	# running beam or using initial skims file path if this is first iteration
	if [[ ( -z "$INITIAL_SKIMS_PATH" ) || ($START_YEAR != ${1})  ]]; then
		BEAM_OUTPUT=$OUTPUT_DATA_PATH/$START_YEAR/beam
		URBANSIM_DATA=$OUTPUT_DATA_PATH/$START_YEAR/urbansim
		generatedBeamConfig=$(prepareBeamConfig $BEAM_CONFIG $BEAM_OUTPUT $URBANSIM_DATA $START_YEAR)
		echoMilestone 1 "RUNNING BEAM FOR YEAR $START_YEAR with config '$generatedBeamConfig' with urbasim data '$URBANSIM_DATA'"
		cd /beam-project
		/beam/bin/beam --config $generatedBeamConfig
		cd -

		uploadDirectoryToS3 "$BEAM_OUTPUT/*" "$S3_OUTPUT_URL/$START_YEAR/beam" &

		# Find the most recent skims.csv.gz output in the output directory, we add timestamp in the find command to ensure this
		SKIMS_FILEPATH=$(find $BEAM_OUTPUT -name "*.skims.csv.gz" -printf "%T@ %Tc &%p\n"  | sort -r | head -n 1 | cut -d '&' -f 2)
		echo "From beam skim file: $SKIMS_FILEPATH"
	else
		echoMilestone 1 "skipping beam for year $START_YEAR"
		SKIMS_FILEPATH=s3:$INITIAL_SKIMS_PATH

		mkdir -p $OUTPUT_DATA_PATH/$START_YEAR/beam-was-skipped
		echo "initial skim file was taken from $SKIMS_FILEPATH" > $OUTPUT_DATA_PATH/$START_YEAR/beam-was-skipped/skim-file-source.log

		uploadDirectoryToS3 "$OUTPUT_DATA_PATH/$START_YEAR/beam-was-skipped" "$S3_OUTPUT_URL/$START_YEAR/beam-was-skipped" &

		echo "Initial skim file:$SKIMS_FILEPATH"
	fi

	echoMilestone 1

	# What is the end year of the BAUS run
	((END_YEAR = START_YEAR + BEAM_BAUS_ITER_FREQ))

	# Different steps get run if simulating from base-year data
	if [[ $START_YEAR == ${1} ]]; then

		# Make in-year model data .h5 from base data
		echoMilestone "2.1" "MAKING MODEL DATA HDF STORE FOR BAUS"
		cd $PILATES_PATH/scripts \
			&& $CONDA_DIR/envs/$CONDA_ENV_BAUS_ORCA_1_4/bin/python make_model_data_hdf.py \
			-m -b -i $BAUS_INITIAL_DATA \
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
			-m -i $INPUT_DATA_PATH/$START_YEAR \
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
			-o $OUTPUT_DATA_PATH/$START_YEAR/urbansim -x \
			&& echoMilestone "4.3"

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
	echoMilestone 7 "SENDING END-YEAR ACTIVITYSYNTH OUTPUTS TO OUTPUT BUCKET"
	cd $PILATES_PATH/scripts && $CONDA_DIR/envs/$CONDA_ENV_ASYNTH/bin/python \
		make_csvs_from_output_store.py -d $ASYNTH_DATA_OUTPUT_FILEPATH \
		-o $OUTPUT_DATA_PATH/$END_YEAR/urbansim \
		&& echoMilestone 7

	echoMilestone 8 "COPYING END-YEAR ACTIVITYSYNTH OUTPUTS TO INPUT BUCKET"
	cp -r $OUTPUT_DATA_PATH/$END_YEAR/urbansim $INPUT_DATA_PATH/$END_YEAR
	echoMilestone 8

  uploadDirectoryToS3 "$OUTPUT_DATA_PATH/$END_YEAR/urbansim" "$S3_OUTPUT_URL/$END_YEAR/urbansim" &

	((START_YEAR = $START_YEAR + BEAM_BAUS_ITER_FREQ))

done

echoMilestone 10 "RUNNING BEAM FOR YEAR $START_YEAR with config: $BEAM_CONFIG"
BEAM_OUTPUT=$OUTPUT_DATA_PATH/$START_YEAR/beam
URBANSIM_DATA=$OUTPUT_DATA_PATH/$START_YEAR/urbansim
generatedBeamConfig=$(prepareBeamConfig $BEAM_CONFIG $BEAM_OUTPUT $URBANSIM_DATA $START_YEAR)
cd /beam-project
/beam/bin/beam --config $generatedBeamConfig
cd -
echoMilestone 10

uploadDirectoryToS3 "$BEAM_OUTPUT" "$S3_OUTPUT_URL/$START_YEAR/beam" &

# waiting for all background jobs to be finished
wait

echoMilestone

