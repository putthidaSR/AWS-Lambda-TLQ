import boto3
import sqlite3
from Inspector import *
import load 
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

def handler(event, context):
	s3 = boto3.client('s3')
	inspector = Inspenctor()
	inspector.inspectAll()
	inspector.addTimeeStamp("FrameWorkRuntime")
	
	bucket = even.get("bucketname")
	key = even.get("filename")
	data_path = '/tmp/target.csv'
	db_path = '/tmp/'+key.split('.')[0]+'.db'
	
	s3.download_file(bucket, key, data_path)
	load.database_init(data_path, db_path, logger)
	s3.upload_file(db_path, bucket, 'target.db')

	inspector.inspectCPUAll()
	inspector.addAtrribute("DatabaseName", "target.db")
	return inspector.finish()

