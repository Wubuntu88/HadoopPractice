from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("order_count_by_status")
sc = SparkContext(conf=conf)

orders = sc.textFile('retail_db_flat/orders/part*').map( lambda line: line.split("\t") )

status_order_id_keyval_pair = orders.map( lambda rec: (rec[3], int(rec[0])) )
num_order_by_status = status_order_id_keyval_pair.groupByKey().map( lambda kv: (kv[0], len(kv[1])) )

suspected_fraud = num_order_by_status.filter(lambda kv: kv[0] == 'SUSPECTED_FRAUD')

number_of_suspected_fraud_orders = suspected_fraud.take(1)[0][1]
total_orders = num_order_by_status.aggregate(0, \
					lambda total, kv: total + kv[1], \
					lambda sum1, sum2: sum1 + sum2)

print "*********\nsuspected fraud count: " + str(number_of_suspected_fraud_orders)
print "*********\ntotal orders: " + str(total_orders)

print str(num_order_by_status.take(10))
