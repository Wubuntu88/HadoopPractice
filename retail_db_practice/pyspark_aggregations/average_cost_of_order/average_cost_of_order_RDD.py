from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("average cost of order using RDD")
sc = SparkContext(conf=conf)

order_items = sc.textFile("retail_db_flat/order_items/part-m-00000")

#gets the revenue for all order items; subtotal is index 4
revenue = order_items.map(lambda line: float(line.split("\t")[4])).sum()
#gets the total number of orders
number_of_orders = order_items.map(lambda line: int(line.split("\t")[1])).distinct().count()

#rev = revenue.take(1)
#num_orders = number_of_orders.take(1)
average_order_price = revenue / number_of_orders

print "**********************\nrevenue: {0}".format(revenue)
print "**********************\nnumber of orders: {0}".format(number_of_orders)
print "**********************\naverage order price: {0}\n**********************".format(average_order_price)
