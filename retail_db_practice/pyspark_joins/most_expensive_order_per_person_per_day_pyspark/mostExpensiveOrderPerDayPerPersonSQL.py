from pyspark import SparkContext, SparkConf
from pyspark import HiveContext
'''
Problem statement: find the 5 most expensive orders per person per day using SparkSQL.
'''
conf = SparkConf().setAppName("mostExpensiveOrderPerDayPerPersonSQL")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
#makes sure that the 'retail_db' hive database will be used
hiveContext.sql("use retail_db")

#firstvalue() must be used because of a bug.  
#Without it, columns that are not in the group by or the aggregation part cannot be shown.
sqlString = "SELECT \
			first_value(customers.customer_fname), \
			first_value(customers.customer_lname), \
			orders.order_date, \
			ROUND(SUM(order_items.order_item_subtotal), 2) the_total\
		FROM customers, orders, order_items \
		WHERE 	orders.order_id = order_items.order_item_order_id \
			AND \
			customers.customer_id = orders.order_customer_id \
		GROUP BY orders.order_date, customers.customer_id \
		ORDER BY the_total DESC"

result = hiveContext.sql(sqlString).rdd #rdd used because this is certification practice
top_records = result.take(5)
print "*****************\n{0}".format(str(top_records))



































