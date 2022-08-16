#!/bin/sh

if  [ $# -eq 2 ]
    then
        echo "Uploading results to Google Cloud in Region: $1";
        echo "GCloud directory: $2";

        gcloud alpha storage cp -r -n pilates/beam/beam_output/$1/year-* gs://beam-core-outputs/$2/beam/
        gcloud alpha storage cp -r -n pilates/activitysim/output/final* gs://beam-core-outputs/$2/activitysim/
        gcloud alpha storage cp -r -n pilates/activitysim/output/year* gs://beam-core-outputs/$2/activitysim/
        gcloud alpha storage cp -r -n pilates/activitysim/output/pipeline.h5 gs://beam-core-outputs/$2/activitysim/pipeline.h5
        gcloud alpha storage cp -r -n pilates/activitysim/data/ gs://beam-core-outputs/$2/activitysim/data/
        gcloud alpha storage cp -r -n pilates/postprocessing/output/ gs://beam-core-outputs/$2/inexus/

else
    echo "Please provide a region (e.g. 'austin' or 'sfbay') and GCloud directory name"
fi        
