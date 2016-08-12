from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("customers suspected of fraud")
sc = SparkContext(conf=conf)

orders_with_suspected_fraud = \
	sc.textFile("retail_db_flat/orders/part*")\
		.map( lambda line: line.split("\t") )\
		.filter( lambda comps: comps[3] == "SUSPECTED_FRAUD" )\
		.map( lambda rec: (int(rec[0]), rec) )

order_items = sc.textFile("retail_db_flat/order_items/part*")\
		.map( lambda line: line.split("\t") )\
		.map( lambda record: (int(record[1]), record) )

orders_join_order_items = orders_with_suspected_fraud.join(order_items)
'''
example format of orders_join_order_items
(65652, 
	([u'65652', u'2014-05-21 00:00:00', u'7192', u'SUSPECTED_FRAUD'], 	#order
	[u'164071', u'65652', u'502', u'1', u'50', u'50']))			#order_item
	--customer_id is item [2] in order_item
'''

customer_id_and_date_total_kv_pair = \
	orders_join_order_items.map( 	lambda kv: ( kv[1][1][2], # customer_id (key)
							(kv[1][0][1], # date value_tuple[0]
							 kv[1][1][4]) # subtotal value_tuple[1]
						) 
					)
customers_by_customer_id = sc.textFile("retail_db_flat/customers/part*")\
	.map( lambda line: line.split("\t") )\
	.map( lambda record: (record[0], record) )

date_total_pair_join_customers = \
	customer_id_and_date_total_kv_pair.join(customers_by_customer_id)
'''
(u'249', 
	((u'2014-04-26 00:00:00', u'109.94'), 
	[u'249', u'Mary', u'Randolph', u'XXXXXXXXX', u'XXXXXXXXX', u'6026 Stony Elk Hill', u'Portland', 		u'OR', u'97206']))
'''

# first group by the customer_id and then aggregate the subtotals
suspected_fraud_totals_by_cust_id = \
	date_total_pair_join_customers.map(lambda kv: (kv[0], #cust_id
							(float(kv[1][0][1]), # subtotal
							kv[1][1][1], # f_name
							kv[1][1][2], # l_name
							1)) #count
						)
def add_subtotals(tot_fname_lname_tuple, other):
    new_tuple = (float(tot_fname_lname_tuple[0]) + float(other[0]), 
		tot_fname_lname_tuple[1], 
		tot_fname_lname_tuple[2],
		tot_fname_lname_tuple[3] + other[3])
    return new_tuple

agg_by_cust_id = suspected_fraud_totals_by_cust_id.combineByKey(lambda tup: tup,
								add_subtotals,
								add_subtotals)

agg_by_cust_id_sorted_by_total = agg_by_cust_id.sortBy(keyfunc=lambda kv: kv[1][0], ascending=False)
print "**************\n" + str(agg_by_cust_id_sorted_by_total.take(5))
'''
let's look at the top 5 poeple
(customer_id, (total_spent, fname, lname, number of items purchased))
[(u'1004', (157592.12000000026, u'Mary', u'Evans', 394)), 
(u'365', (101563.07000000017, u'Shirley', u'Smith', 560)), 
(u'957', (91493.899999999951, u'Eric', u'Jones', 305)), 
(u'191', (79392.060000000085, u'Victoria', u'Smith', 267)), 
(u'502', (70450.0, u'Jennifer', u'May', 481))]

Looks like there are some pretty big spenders...
$70k to 150k is a lot to spend at a sporting goods store.
Maybe there are legitimate frauds, haha =).
'''






























