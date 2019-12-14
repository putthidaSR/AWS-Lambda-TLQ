#!/bin/bash

# JSON object to pass to Lambda Function
json={"\"bucketname\"":"\"tcss562-fall2019-group7\"","\"filename\"":"\"SalesRecordsDB/transformData.db\"","\"filter\"":"\"Region='Europe'\u0020AND\u0020Order_Priority='Medium'\"","\"aggregation\"":"\"AVG(Order_Processing_Time),AVG(Gross_Margin),AVG(Units_Sold),MAX(Units_Sold),MIN(Units_Sold),SUM(Units_Sold),SUM(Total_Revenue),SUM(Total_Profit),COUNT(Order_ID)\"","\"groupBy\"":"\"Region\""}

echo $json
echo "Invoking Service 3 - Data Filtering and Aggregation"

time output=`aws lambda invoke --invocation-type RequestResponse --function-name service3_data_filter_aggregation --region us-east-1 --payload $json /dev/stdout | head -n 1| sed 's/.$//'; echo`

echo ""
echo "INVOKE RESULT:"
echo $output | jq
echo ""
