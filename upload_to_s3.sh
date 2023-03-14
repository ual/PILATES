#!/bin/sh

if  [ $# -eq 2 ]
    then
        echo "Uploading results to S3 in Region: $1";
        echo "S3 directory: $2";

        aws s3 sync pilates/beam/beam_output/$1/ s3://beam-outputs/pilates-outputs/$2/beam/ --region us-east-2 --exclude "*xml*" --include "year-*" ;
        aws s3 sync pilates/activitysim/output/ s3://beam-outputs/pilates-outputs/$2/activitysim/ --region us-east-2 --exclude "*" --include "final*" --include "year*";
        aws s3 cp pilates/activitysim/output/pipeline.h5 s3://beam-outputs/pilates-outputs/$2/activitysim/pipeline.h5 --region us-east-2;
        aws s3 sync pilates/activitysim/data/ s3://beam-outputs/pilates-outputs/$2/activitysim/data/ --region us-east-2;
#        aws s3 sync pilates/beam/beam_output/ s3://beam-outputs/pilates-outputs/"$2"/ --exclude "*" --include "*$1*" --region us-east-2;
        aws s3 sync pilates/postprocessing/output/ s3://beam-outputs/pilates-outputs/$2/inexus/ --region us-east-2 --exclude ".git*"
        aws s3 sync pilates/postprocessing/MEP/ s3://beam-outputs/pilates-outputs/$2/MEP/ --region us-east-2 --exclude ".git*"
else
    echo "Please provide a region (e.g. 'austin' or 'sfbay') and S3 directory name"
fi        
