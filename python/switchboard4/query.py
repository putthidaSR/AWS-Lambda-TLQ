import sqlite3
from sqlite3 import Error
import json

def connect(db_path):
    try:
        return sqlite3.connect(db_path)
    except Error as e:
        print(e)
        return None
        

def search(db_path, where, group):
    con = connect(db_path)
    sql = "SELECT {},\
                AVG(OrderProcessingTime) as avg_processTime,\
                AVG(GrossMargin) as avg_grossMargin, \
                AVG(UnitsSold) as avg_unitsSold,\
                MAX(UnitsSold) as max_unitsSold,\
                MIN(UnitsSold) as min_unitsSold,\
                SUM(UnitsSold) as total_unitsSold,\
                SUM(TotalRevenue) as total_revenue,\
                SUM(TotalProfit) as total_profit,\
                COUNT(OrderID) as num_orders\
                FROM sales WHERE {} \
                GROUP BY {};".format(group, where, group)
    
    cursor = con.cursor()
    result = cursor.execute(sql)
    
    items = [dict(zip([key[0] for key in cursor.description], row)) for row in result]
            
    return json.dumps(items), len(items)