from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext
from operator import add

conf = SparkConf().setAppName("revenueByDay")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

orders = sqlContext.sql("select order_id, order_date from retail_db.orders").rdd
order_items = sqlContext.sql("select order_item_order_id, order_item_subtotal from retail_db.order_items").rdd
#(order_id, order_date)
orders_id_date_kv = orders.map(lambda rec: (rec[0], rec[1]))
#(order_items_order_id, subtotal)
order_items_id_subtotal_kv = order_items.map(lambda rec: (rec[0], rec[1]))

orders_join_order_items = orders_id_date_kv.join(order_items_id_subtotal_kv)

revenue_per_order_per_day = orders_join_order_items.map(lambda t: (t[1][0], t[1][1]))

revenue_per_day = revenue_per_order_per_day.reduceByKey(add)

lowest_revenue_days_5 = revenue_per_day.takeOrdered(5, lambda tup: tup[1])
output1 =  "****************************\n{0}\n****************************".format(str(lowest_revenue_days_5))
highest_revenue_days_5 = revenue_per_day.takeOrdered(5, lambda tup: -tup[1])
output2 =  "****************************\n{0}\n****************************".format(str(highest_revenue_days_5))

print "{0}\n{1}".format(output1, output2)






















