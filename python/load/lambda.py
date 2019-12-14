import boto3
import sqlite3
from Inspector import *
import load 
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
	s3 = boto3.client('s3')
	inspector = Inspector()
	inspector.inspectAll()
	inspector.addTimeStamp("FrameWorkRuntime")
	
	bucket = event.get("bucketname")
	key = event.get("filename")
	data_path = '/tmp/target.csv'
	db_path = '/tmp/'+key.split('.')[0]+'.db'
	
	s3.download_file(bucket, key, data_path)
	load.database_init(data_path, db_path, logger)
	s3.upload_file(db_path, bucket, 'target.db')

	inspector.inspectCPUDelta()
	inspector.addAttribute("DatabaseName", "target.db")
	return inspector.finish()

