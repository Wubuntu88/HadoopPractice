from pyspark import SparkContext, SparkConf

def get_id_subtotal_pairs(line):
    comps = line.split("\t")
    return ( int(comps[0]), float(comps[4]) )

conf = SparkConf().setAppName("average cost of item per order sorted")
sc = SparkContext(conf=conf)

order_items = sc.textFile("retail_db_flat/order_items/part-m-00000")

id_subtotal = order_items.map(get_id_subtotal_pairs)

revenue_item_count_per_order_by_order_id = id_subtotal.combineByKey(lambda value: (value, 1), \
						lambda agg_tuple, value_tuple: \
							(agg_tuple[0] + value_tuple[0], agg_tuple[1] + 1), \
						lambda agg_tuple1, agg_tuple2: \
							(agg_tuple1[0] + agg_tuple2[0], \
							agg_tuple1[1] + agg_tuple2[1]) \
							)
average_cost_per_item_per_order = \
	revenue_item_count_per_order_by_order_id.map(lambda key_val: (key_val[0], \
									key_val[1][0] / key_val[1][1]))

average_cost_per_item_per_order_sorted = \
	average_cost_per_item_per_order.sortBy(keyfunc=lambda kv: kv[1], ascending=False)

print "*********\n{0}".format(average_cost_per_item_per_order_sorted.take(20))



























