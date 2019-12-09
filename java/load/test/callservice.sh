#!/bin/bash

# JSON object to pass to Service 2 - Data Load
json={"\"bucketname\"":"\"tcss562.project.fall19\"","\"filename\"":"\"TransformedData/$1\u0020Sales\u0020Records.csv\""}

echo $json
echo "Invoking Service 2 - Data Load using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service2_data_load --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""
echo "INVOKE RESULT:"
echo $output | jq
echo ""
