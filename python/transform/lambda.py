# comment out when deploy
import sys
sys.path.append('python/lib/python3.7/site-packages/')

import pandas as pd
import boto3
from Inspector import *
import Transform as trans

def handler(event, context):
	s3_client = boto3.client('s3')
	inspector = Inspector()
	inspector.addTimeStamp("FramworkRuntime")

	bucket = event.get("bucketname")
	key = event.get("filename")

	filename = '/tmp/target.csv'
	processed_file = '/tmp/processed.csv'
	upload_file = 'transform.csv'
	s3_client.download_file(bucket, key, filename)
	processed_data = trans.process(filename)
	processed_data.to_csv(processed_file)
	s3_client.upload_file(processed_file, bucket, upload_file)

	inspector.inspectCPUAll()
	inspector.addAttribute("isSuccess", "true")
	return inspector.finish()
