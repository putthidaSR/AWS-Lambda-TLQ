# Transform process

# comment out when deploy
import sys
sys.path.append('../pandas-layers/python/lib/python3.7/site-packages/')

import pandas as pd
def process(filename):
	data_type = {"Order Id":"int64", "Units Sold":"int64",\
	"Unit Price":"float64", \
	"Unit Cost":"float64", "Totoal Revenue":"float64",\
	"Total Cost":"float64", "Total Profit":"float64"}
	data = pd.read_csv(filename, dtype=data_type)

	# drop duplicated rows
	data = data.sort_values('Order Date', ascending=False)
	data = data.drop_duplicates(subset='Order ID', keep='first')
	
	# cal time diff, margin, convering priority and adding new cols
	for i, row in data.iterrows():
		data.set_value(i, 'Order Processing Time', \
				(pd.to_datetime(row['Ship Date'], format="%m/%d/%Y")\
					- pd.to_datetime(row['Order Date'], format="%m/%d/%Y")).days)
		data.set_value(i, 'Gross Margin', row['Total Profit']/row['Total Revenue'])
		if row['Order Priority'] == 'L':
			data.set_value(i, 'Order Priority', "Low")
		elif row['Order Priority'] == 'M':
			data.set_value(i, 'Order Priority',"Medium")
		elif row['Order Priority'] == 'H':
			data.set_value(i, 'Order Priority', "High")
		elif row['Order Priority'] == 'C':
			data.set_value(i, 'Order Priority', "Critical")
		else:
			data.set_value(i, 'Order Priority', None)
	
	return data

if __name__ == "__main__":
	filename = '../sample_data/100_Sales_Records.csv'
	data = process(filename)
	outputFile = 'processed.csv'
	data.to_csv(outputFile, index=False)
