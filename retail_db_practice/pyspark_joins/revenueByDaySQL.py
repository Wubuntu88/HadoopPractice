from pyspark import SparkContext, SparkConf
from pyspark import HiveContext

conf = SparkConf().setAppName("revenueByDaySQL")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use retail_db")
sqlString = "SELECT 	orders.order_date, \
			ROUND(SUM(order_items.order_item_subtotal), 2) the_sum, \
			COUNT(DISTINCT orders.order_id) the_count\
		FROM orders, order_items \
		WHERE orders.order_id = order_items.order_item_order_id \
		GROUP BY orders.order_date \
		ORDER BY the_sum"

joinded_aggregate_data = hiveContext.sql(sqlString)

print str(joinded_aggregate_data.take(5))










































