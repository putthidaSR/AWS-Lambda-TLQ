#!/bin/bash

# JSON object to pass to Service 1 - Data Transformation
json={"\"bucketname\"":"\"tcss562.project.fall19\"","\"filename\"":"\"OriginalSalesRecords.csv\""}

echo $json
echo "Invoking Service 1 using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service1_data_transform --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""
echo "INVOKE RESULT:"
echo $output | jq
echo ""
