from pyspark import SparkContext, SparkConf
from pyspark import HiveContext
from operator import add
'''
Problem statement: find the 5 most expensive orders per person per day using rdds
Unfortunately, many map() operations need to be made for the joins and aggregations.
This means that it is hard to keep track of the indices.
I did this one with RDD, but have also done it with data frames and Spark SQL.
Data Frames and Spark SQL are much easier; I would recommend those approaches.
You can wade through the swamp wasteland of RDD remapping and joining if you want.
'''

conf = SparkConf().setAppName("mostExpensiveOrderPerDayPerPersonRDD")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use retail_db")

orders = hiveContext.sql("select order_id, order_date, order_customer_id from orders").rdd
customers = hiveContext.sql("select customer_id, customer_fname, customer_lname from customers").rdd
order_items = hiveContext.sql("select order_item_order_id, order_item_subtotal from order_items").rdd

orders_customer_id_map = orders.map(lambda rec: (rec[2], rec))
customers_id_map = customers.map(lambda tup: (tup[0], tup))
orders_join_customers = orders_customer_id_map.join(customers_id_map)

orders_customers_by_order_id = orders_join_customers.map(lambda rec: (rec[1][0][0], (rec[1][0], rec[1][1])))

order_items_by_order_id = order_items.map(lambda rec: (rec[0], rec)) 

orders_join_order_items = orders_customers_by_order_id.join(order_items_by_order_id)


date_cust_id_key = orders_join_order_items.map(lambda rec: ((rec[1][0][0][1], rec[1][0][0][2]),rec[1][1][1]))

agg_by_key = date_cust_id_key.reduceByKey(add)

mapped_by_cust_id = agg_by_key.map(lambda x: (x[0][1], (x[0][0], x[1])) )

sum_join_customers = mapped_by_cust_id.join(customers_id_map)

final = sum_join_customers.map(lambda x: (x[1][1][1], x[1][1][2], x[1][0][0], x[1][0][1]))

print "******************\n{0}\n".format(final.takeOrdered(5, key=lambda x: -x[3]))









