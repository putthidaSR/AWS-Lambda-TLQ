#!/bin/bash

# JSON object to pass to Service 1 - Data Transformation
json={"\"bucketName\"":"\"tcss562-fall2019-group7\"","\"fileName\"":"\"5000-Sales-Records.csv\""}

echo $json
echo "Invoking Service 1 using AWS CLI"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service1_data_transformation --region us-east-1 --payload $json --cli-connect-timeout 0 /dev/stdout | head -n 1| sed 's/.$//'; echo`

echo ""
echo "INVOKE RESULT FOR FIRST TIME:"
echo $output | jq
echo ""

echo "Calculate Average Runtime"

runtime=0.0
count=1
maxRun=100
while [ $count -le $maxRun ]
    do
        output=`aws lambda invoke --invocation-type RequestResponse --function-name service1_data_transformation --region us-east-1 --payload $json --cli-connect-timeout 0 /dev/stdout | head -n 1| sed 's/.$//'; echo`
        runtime_one=`echo $output | jq '.runtime'`
        # echo $runtime_one
        runtime=`echo "$runtime_one + $runtime" | bc -l`
        # ((runtime += $runtime_one))
        ((count++))
    done

echo "Avg runtime:"
echo "$runtime/$maxRun" | bc -l

