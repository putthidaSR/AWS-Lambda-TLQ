# comment out when deploy
'''
import sys
sys.path.append('python/lib/python3.7/site-packages/')
'''

import pandas as pd
import boto3
from Inspector import *
import Transform as trans

def handler(event, context):
	s3_client = boto3.client('s3')
	inspector = Inspector()
	inspector.inspectAll()
	inspector.addTimeStamp("FramworkRuntime")

	bucket = event.get("bucketname")
	key = event.get("filename")

	filename = '/tmp/target.csv'
	processed_file = '/tmp/processed.csv'
	upload_key = 'transform.csv'
	s3_client.download_file(bucket, key, filename)
	processed_data = trans.process(filename)
	processed_data.to_csv(processed_file, index=False)
	s3_client.upload_file(processed_file, bucket, upload_key)

	inspector.inspectCPUDelta()
	inspector.addAttribute("outputFile", upload_key)
	inspector.addAttribute("numLine", processed_data.shape[0])
	return inspector.finish()
