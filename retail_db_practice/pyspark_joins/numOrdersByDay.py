from pyspark import SparkContext, SparkConf
from pyspark.sql import HiveContext

conf = SparkConf().setAppName("ordersPerDay")
sc = SparkContext(conf=conf)
sqlContext = HiveContext(sc)

orders = sqlContext.sql("select order_id, order_date from retail_db.orders").rdd
order_items = sqlContext.sql("select order_item_order_id from retail_db.order_items").rdd

#rec[0] is the position of the order_id
ordersById = orders.map(lambda rec: (rec[0], rec))
#rec[0] is the position of the order_item's order_id
orderItemsByOrderId = order_items.map(lambda rec: (rec[0], rec))

ordersJoinOrderItems = ordersById.join(orderItemsByOrderId)
#t[1][0][1] is the date; t[1][0][4] is the subtotal
revenuePerOrderPerDay = ordersJoinOrderItems.map(lambda t: (t[1][0][1], t[1][1][0]))

#Get the order count per day
#rec[1][0][1] is the date, rec[0] is the order_id
ordersPerDay = ordersJoinOrderItems.map(lambda rec: (rec[1][0][1], rec[0]))
keyCounts = ordersPerDay.aggregateByKey(0, lambda x,y: x + 1, lambda x, y: x + y)
counts = keyCounts.map(lambda tup: tup[1]).cache()

stdev = counts.stdev()
mean = counts.mean()
sc.broadcast(stdev)
sc.broadcast(mean)

the_max = counts.max()
the_min = counts.min()

outliers = keyCounts.filter(lambda tup: tup[1] >= mean + 2 * stdev or tup[1] <= mean - 2 * stdev)

outputStr = ["//*********************//\n"]
outputStr += "//*********************//\n\n"
outputStr += "orders per day: " + str(keyCounts.takeOrdered(5, key=lambda tup: -tup[1])) + "\n"
outputStr += "stdev: {0}\n".format(stdev)
outputStr += "mean: {0}\n".format(mean)
outputStr += "max: {0}\n".format(the_max)
outputStr += "min: {0}\n".format(the_min)
outputStr += "outliers: " + str(outliers.take(5)) + "\n"
outputStr += "\n//*********************//"
outputStr += "\n\n//*********************//"
print "".join(outputStr)
























