#!/bin/bash

set -e

export AWS_DEFAULT_REGION=us-west-2
export AWS_DEFAULT_OUTPUT=json

aws sts assume-role \
    --role-arn "arn:aws:iam::202306134469:role/datamechanics-tf" \
    --role-session-name assumed-role-session > /tmp/role_response.json

export AWS_ACCESS_KEY_ID=$(cat /tmp/role_response.json | jq .Credentials.AccessKeyId | tr -d '"')
export AWS_SECRET_ACCESS_KEY=$(cat /tmp/role_response.json | jq .Credentials.SecretAccessKey | tr -d '"')
export AWS_SESSION_TOKEN=$(cat /tmp/role_response.json | jq .Credentials.SessionToken | tr -d '"')

aws s3 cp --recursive s3://dataeng-spark-jobs/dm-r/ /opt/spark/work-dir/

unset AWS_ACCESS_KEY_ID
unset AWS_SECRET_ACCESS_KEY
unset AWS_SESSION_TOKEN

# NOTE: this entrypoint is specific to gcr.io/datamechanics/spark-connectors:3.0.0-dm4
# Data Mechanics entrypoint
now=$(date +"%T")
echo "Unsetting extraneous env vars (UTC): $now"
unset `env | grep _UI_SVC_ | cut -d = -f 1`
now=$(date +"%T")
echo "Finished unsetting extraneous env vars (UTC): $now"
# Actually start the Spark entrypoint
exec /usr/bin/tini -s -- /opt/entrypoint.sh "$@"
