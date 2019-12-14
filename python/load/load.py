import sqlite3
import csv
from sqlite3 import Error
import os.path

# comment out when deploy
import logging

def create_table(con, logger):
	sql_create_table = """ CREATE TABLE IF NOT EXISTS sales (
							Region text, 
							Country text,
							ItemType text,
							SalesChannel text,
							OrderPriority text,
							OrderDate date,
							OrderID integer PRIMARY KEY,
							ShipDate date,
							UnitsSold integer,
							UnitPrice float,
							UnitCost float,
							TotalRevenue float,
							TotalCost float,
							TotalProfit float,
							OrderProcessingTime integer,
							Gross Margin float
							);"""
	try:
		c = con.cursor()
		c.execute(sql_create_table)
	except Error as e:
		print(e)
		logger.info(e)

def insert_data(con, row):
	sql = """ INSERT INTO sales VALUES 
			(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"""
	cur = con.cursor()
	cur.execute(sql, row)

def database_init(data_path, db_path, logger):
	# create connection / create database
	BASE_DIR = os.path.dirname(os.path.abspath(__file__))
	db_path = os.path.join(BASE_DIR, db_path)
	con = None
	try:
		con = sqlite3.connect(db_path)
		con.text_factory = sqlite3.OptimizedUnicode
	except Error as e:
		print(e)
		logger.info(e)

	# create table
	if con is not None:
		create_table(con, logger)
	else:
		print("Error cannot create the connection.")

	# insert data
	con.commit()
	with open(data_path) as csv_file:
		data = csv.reader(csv_file)
		line = 0
		for row in data:
			if line == 0:
				line += 1
			else:
				insert_data(con, row)
				line += 1
	con.commit()
	
'''
if __name__ == '__main__':
	logger = logging.getLogger()
	logger.setLevel(logging.INFO)
	database_init('transform.csv','test.db', logger)
'''
