import pandas as pd
import boto3
import Transform as trans
import load
import query
from Inspector import *
import logging
import os

def handler(event, context):
	s3 = boto3.client('s3')
	inspector = Inspector()
	inspector.inspectAll()
	inspector.addTimeStamp("FrameWorkRuntime")

	service = event.get("service")
	bucket = event.get("bucketname")
	key = event.get("filename")

	if service == 1:
		filename = '/tmp/target.csv'
		processed_file = '/tmp/processed.csv'
		upload_key = 'transform.csv'
		s3.download_file(bucket, key, filename)
		processed_data = trans.process(filename)
		processed_data.to_csv(processed_file, index=False)
		s3.upload_file(processed_file, bucket, upload_key)
		inspector.addAttribute("numLine", processed_data.shape[0])
		inspector.addAttribute("outputFile", upload_key)
	elif service == 2:
		logger = logging.getLogger()
		logger.setLevel(logging.INFO)
		data_path = '/tmp/target.csv'
		db_path = '/tmp/'+key.split('.')[0]+'.db'

		s3.download_file = 'transform.csv'
		load.database_init(data_path, db_path, logger)
		s3.upload_file(db_path, bucket, 'target.db')
		inspector.addAttribute("DatabaseName", "target.db")
	elif service == 3:
		where_statement = event.get("where")
		group_statement = event.get("group")
		db_path = '/tmp/target.db'
		if not os.path.isfile(db_path):
			s3.download_file(bucket, key, db_path)
		result, lines = query.search(db_path, where_statement, group_statement)
		inspector.addAttribute("data", result)
		inspector.addAttribute("numLine", lines)
	else:
		raise NameError("There is no such service")
	
	inspector.inspectCPUDelta()
	return inspector.finish()
