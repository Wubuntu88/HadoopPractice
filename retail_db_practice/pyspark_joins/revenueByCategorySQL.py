from pyspark import SparkConf, SparkContext, HiveContext

conf = SparkConf().setAppName("Revenue per category")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)

hiveContext.sql("use retail_db")

sqlString = 	"SELECT	first_value(cat.category_name), \
			round(sum(oi.order_item_subtotal), 2)  category_revenue\
		FROM 	categories cat, products prod, order_items oi \
		WHERE 	cat.category_id = prod.product_category_id  \
			AND \
			prod.product_id = oi.order_item_product_id \
		GROUP BY cat.category_id \
		ORDER BY category_revenue DESC"

result = hiveContext.sql(sqlString)
collected = result.collect()
print "*****************\n{0}".format("\n".join([str(x) for x in collected]))



















