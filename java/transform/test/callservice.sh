#!/bin/bash

# JSON object to pass to Service 1 - Data Transformation
json={"\"bucketName\"":"\"tcss562-fall2019-group7\"","\"fileName\"":"\"10000-Sales-Records.csv\""}

echo $json
echo "Invoking Service 1 using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service1_data_transformation --region us-east-1 --payload $json /dev/stdout ; echo`

echo ""
echo "INVOKE RESULT:"
echo $output | jq
echo ""
