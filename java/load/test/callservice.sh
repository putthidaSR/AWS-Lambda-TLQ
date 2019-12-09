#!/bin/bash

# JSON object to pass to Service 2 - Data Load
json={"\"bucketName\"":"\"tcss562-fall2019-group7\"","\"fileName\"":"\"TransformedData.csv\""}

echo $json
echo "Invoking Service 2 - Data Load using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service2_data_load --region us-east-1 --payload $json /dev/stdout ; echo`

echo ""
echo "INVOKE RESULT:"
echo $output | jq
echo ""
