from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("order_count_by_status")
sc = SparkContext(conf=conf)

'''
Problem statement: 
	Find the number of order with suspected fraud.
	Give the percent of the orders with suspected fraud compared to the total.
'''

orders = sc.textFile('retail_db_flat/orders/part*').map( lambda line: line.split("\t") )

status_order_id_keyval_pair = orders.map( lambda rec: (rec[3], int(rec[0])) )
#note group by is inneficient; this script is just for practice
num_order_by_status = status_order_id_keyval_pair.groupByKey().map( lambda kv: (kv[0], len(kv[1])) )

suspected_fraud = num_order_by_status.filter(lambda kv: kv[0] == 'SUSPECTED_FRAUD')

#the count is wrapped in a list after the take method, so I unwrap it with [0] 
#and index into the value position, which is [1]
number_of_suspected_fraud_orders = suspected_fraud.take(1)[0][1]
#note aggregating after a group by is inneficient; this script is just practice.
total_orders = num_order_by_status.aggregate(0, \
					lambda total, kv: total + kv[1], \
					lambda sum1, sum2: sum1 + sum2)

displayStr = 	"*********\nsuspected fraud count: " + str(number_of_suspected_fraud_orders) + \
		"*********\ntotal orders: " + str(total_orders) + \
		"*********\nsuspected fraud is {0:.2}% of purchases".format( \
								float(number_of_suspected_fraud_orders) / \
								total_orders)
print displayStr










