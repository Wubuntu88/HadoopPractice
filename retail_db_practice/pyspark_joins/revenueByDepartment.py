from pyspark import SparkConf, SparkContext, HiveContext

conf = SparkConf().setAppName("revenue by department")
sc = SparkContext(conf = conf)
hiveContext = HiveContext(sc)
hiveContext.sql("use retail_db")

sqlString = 	"SELECT \
			first_value(dept.department_name), \
			sum(oi.order_item_subtotal) department_revenue\
		FROM \
			departments dept, categories cat, products prod, order_items oi \
		WHERE 	\
			dept.department_id = cat.category_department_id \
			AND \
			cat.category_id = prod.product_category_id \
			AND \
			prod.product_id = oi.order_item_product_id \
		GROUP BY \
			dept.department_id \
		ORDER BY department_revenue DESC"

result = hiveContext.sql(sqlString)
top_records = result.take(5)
print "*****************\n{0}".format(str(top_records))























