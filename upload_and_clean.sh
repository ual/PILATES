#!/bin/sh

echo "Region: $1";
echo "S3 directory: $2";

aws s3 sync pilates/beam/beam_output/$1/ s3://beam-outputs/pilates-outputs/$2/beam/ --region us-east-2 --exclude "*xml*"
aws s3 sync pilates/activitysim/output/ s3://beam-outputs/pilates-outputs/$2/activitysim/ --region us-east-2 --exclude "*" --include "final*"
aws s3 cp pilates/activitysim/output/pipeline.h5 s3://beam-outputs/pilates-outputs/$2/activitysim/pipeline.h5 --region us-east-2
aws s3 sync pilates/activitysim/data/ s3://beam-outputs/pilates-outputs/$2/activitysim/data/ --region us-east-2

for d in pilates/beam/beam_output/*/ ; do
    echo "deleting $d";
    sudo rm -rf "$d"*
done

sudo rm pilates/activitysim/output/*
sudo rm pilates/activitysim/data/*
