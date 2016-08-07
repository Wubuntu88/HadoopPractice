from pyspark import SparkContext, SparkConf
from pyspark import HiveContext
'''
Problem statement: find the 5 most expensive orders per person per day using Data Frames
'''

conf = SparkConf().setAppName("mostExpensiveOrderPerDayPerPersonRDD")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)
#makes sure that the 'retail_db' hive database will be used
hiveContext.sql("use retail_db")

#loading the data from hive into dataframes
orders = hiveContext.sql("select order_id, order_date, order_customer_id from orders")
customers = hiveContext.sql("select customer_id, customer_fname, customer_lname from customers")
order_items = hiveContext.sql("select order_item_order_id, order_item_subtotal from order_items")

#joining the customers with orders on customer_id.  Orders and customers are the smaller tables
#so I try to join small tables with other small tables before joining to a big table.
orders_join_customers = orders.join(customers, orders.order_customer_id == customers.customer_id)

#joining on order_id so that I get rows with a customer and their purchases
orders_customers_join_order_items = \
	orders_join_customers.join(order_items, orders_join_customers.order_id == \
							order_items.order_item_order_id)
#aggregating by order_date and customer_id with the sum aggregation.
#This finds how much a person spent on a single day
aggResult = orders_customers_join_order_items.groupBy(['order_date','customer_id']).agg({"order_item_subtotal": "sum"}).withColumnRenamed("sum(order_item_subtotal)", "subtotal_sum")

#because in the aggregation the order_date, customer, and sum were generated in a data frame,
#I must unfortunatly rejoin to the customers table to display the purchases with names
rejoined_again = aggResult.join(customers, aggResult.customer_id == customers.customer_id)

#I select only the columns that I want
final = rejoined_again.select('customer_fname', 'customer_lname', 'order_date', 'subtotal_sum')

#I sort the data frame so that the highest spenders in a day are at the top
final_sorted = final.sort("subtotal_sum", ascending=False)

#prints out the top 5 spenders in a day
print "******************\norder key: {0}\n".format(final_sorted.take(5))






















