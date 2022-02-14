#!/bin/sh

if  [ $# -eq 2 ]
    then
        echo "Uploading results to S3 in Region: $1";
        echo "S3 directory: $2";

        aws s3 sync pilates/beam/beam_output/$1/ s3://beam-outputs/pilates-outputs/$2/beam/ --region us-east-2 --exclude "*xml*"
        aws s3 sync pilates/activitysim/output/ s3://beam-outputs/pilates-outputs/$2/activitysim/ --region us-east-2 --exclude "*" --include "final*"
        aws s3 cp pilates/activitysim/output/pipeline.h5 s3://beam-outputs/pilates-outputs/$2/activitysim/pipeline.h5 --region us-east-2
        aws s3 sync pilates/activitysim/data/ s3://beam-outputs/pilates-outputs/$2/activitysim/data/ --region us-east-2
fi        
echo "Deleting beam outputs"
for d in pilates/beam/beam_output/*/ ; do
    echo "deleting $d";
    sudo rm -rf "$d"*
done

echo "Deleting activitysim output"

sudo rm pilates/activitysim/output/*

echo "Deleting interim activitysim inputs"
sudo rm pilates/activitysim/data/*

echo "Deleting interim urbansim data"
sudo rm pilates/urbansim/data/input*
sudo rm pilates/urbansim/data/model*
