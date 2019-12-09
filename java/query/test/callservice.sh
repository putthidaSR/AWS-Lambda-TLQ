#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"bucketname\"":"\"tcss562.project.fall19\"","\"filename\"":"\"SalesRecordsDB/$1_Sales_Records.csv.db\"","\"filter\"":"\"Region='Europe'\u0020AND\u0020Order_Priority='Medium'\"","\"aggregation\"":"\"AVG(Gross_Margin),AVG(Order_Processing_Time)\""}

echo $json
echo "Invoking Service 3 - Data Query using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service3_filtering_and_aggregation --region us-east-1 --payload $json /dev/stdout | head -n 1 | head -c -2 ; echo`

echo ""
echo "AWS CLI RESULT:"
echo $output | jq
echo ""
