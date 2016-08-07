from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("most expensive product using RDD and flat files")
sc = SparkContext(conf=conf)

def create_price_name_tuples(line):
    '''
    This function is to be used as the parameter to the map function which
        is called on the RDD that is loaded from the products table (as a textfile)
        It will split the line along the tab delimiter and returns a (product_name, product_price)
        tuple for each line.
        Note: product_name at index 2, product_price at index 4
    '''
    components = line.split("\t")
    return (components[2], float(components[4]))

def get_max_price(tup1, tup2):
    '''
    This is to be used in a reduce function where each input tuple has the structure of 
        (product_name, product_price).  The tuple with the maximum price will be returned.
    #tup[0] is name, tup[1] is price
    '''
    if tup1[1] > tup2[1]:
        return tup1
    else:
        return tup2

products_raw = sc.textFile("hdfs://quickstart.cloudera:8020/user/cloudera/retail_db_flat/products/part-m-00000")

price_name_pair = products_raw.map(create_price_name_tuples)

most_expensive_item = price_name_pair.reduce(lambda x, y: get_max_price(x, y))

print "**************************\n{0}".format(str(most_expensive_item))










