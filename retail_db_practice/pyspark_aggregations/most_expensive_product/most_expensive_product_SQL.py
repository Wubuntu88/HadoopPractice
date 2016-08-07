from pyspark import SparkContext, SparkConf, HiveContext

conf = SparkConf().setAppName("most expensive product using SQL")
sc = SparkContext(conf=conf)
hiveContext = HiveContext(sc)

sqlString = 	"SELECT p.product_name, p.product_price \
		FROM retail_db.products p \
		JOIN 	(SELECT max(products.product_price) max_id \
			FROM retail_db.products) the_max \
			ON \
			p.product_price = the_max.max_id"

result = hiveContext.sql(sqlString)

print("***********************\n{0}".format(str(result.take(1))))
