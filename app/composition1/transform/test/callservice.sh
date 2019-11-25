#!/bin/bash

# JSON object to pass to Service 1 - Data Transformation
json={"\"bucketname\"":"\"tcss562.project.fall19\"","\"filename\"":"\"OriginalSalesRecords.csv\""}

echo $json
echo "Invoking Service 1 using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name composition1transform --region us-east-2 --payload $json /dev/stdout ; echo`

echo ""
echo "INVOKE RESULT:"
echo $output | jq
echo ""
