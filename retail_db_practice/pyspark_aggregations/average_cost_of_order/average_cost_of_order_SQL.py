from pyspark import SparkContext, SparkConf, HiveContext

conf = SparkConf().setAppName("average cost of order SQL")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use retail_db")

# the reason I do not get the count of orders from the orders tables
# is because there are order_ids in the orders table that have
# no matching order_item_order_id in the order_items table
sqlString = 	"SELECT SUM(oi.order_item_subtotal), \
			count(distinct oi.order_item_order_id) \
		FROM 	order_items oi"
#the row is the only thing wrapped in a list, so we use [0] to take it out
result_rdd = hiveContext.sql(sqlString).rdd
result_row = result_rdd.take(1)[0]

average_order_item_cost = result_row[0] / result_row[1]
print("***********************\n{0}".format(str(average_order_item_cost)))
#result: 597.632287963



