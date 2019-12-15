import boto3
import sqlite3
import json
import os
from Inspector import *
import query


def lambda_handler(event, context):
    s3 = boto3.client('s3')
    inspector = Inspector()
    inspector.inspectAll()
    inspector.addTimeStamp("FrameWorkRuntime")
    
    bucket = event.get("bucketname")
    key = event.get("filename")
    where_statement = event.get("where")
    group_statement = event.get("group")
    db_path = '/tmp/target.db'
    
    if not os.path.isfile(db_path):
        s3.download_file(bucket, key, db_path)
    result, lines = query.search(db_path, where_statement, group_statement)
    inspector.addAttribute('data', result)
    inspector.addAttribute('numLine', lines)
    inspector.inspectCPUDelta()
    return inspector.finish()