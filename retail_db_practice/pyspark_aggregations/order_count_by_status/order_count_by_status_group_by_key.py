from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("order_count_by_status")
sc = SparkContext(conf=conf)

'''
Problem statement: 
	find the number of orders by status.  This method finds it using the inneficient
	groupBy method (just for practice).

'''

orders = sc.textFile('retail_db_flat/orders/part*').map( lambda line: line.split("\t") )

status_order_id_keyval_pair = orders.map( lambda rec: (rec[3], int(rec[0])) )
num_order_by_status = status_order_id_keyval_pair.groupByKey().map( lambda kv: (kv[0], len(kv[1])) )

print str(num_order_by_status.take(10))









