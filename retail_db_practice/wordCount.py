from pyspark import SparkContext, SparkConf

conf = SparkConf().setAppName("word count")
sc = SparkContext(conf=conf)

text = sc.textFile("/user/cloudera/retail_db_flat/customers/part-m-00000")
words = text.flatMap(lambda line: line.split("\t"))
words_one = words.map(lambda x: (x, 1))
words_frequency = words_one.reduceByKey(lambda x, y: x + y)
word_freq_sorted = words_frequency.sortBy(keyfunc=lambda tup: tup[1], ascending=False)
result_formatted.saveAsTextFile('hdfs:///user/cloudera/word_count_result')

