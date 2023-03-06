from pyspark import SparkConf, SparkContext

def hasPython(line):
	return "python" in line

#SparkContext
conf = SparkConf().setMaster("local[*]").setAppName("lab6")
sc = SparkContext(conf=conf)

#1. Create a new RDD from the external file
lines = sc.textFile("lab6_test.txt") #"test.txt"

#2. Transformation
pythonlines = lines.filter(hasPython)

pythonlines.persist

#3. Action
print(pythonlines.first())
print(pythonlines.count())

#4. Save files
pythonlines.saveAsTextFile("output")

#RDD_from_list

rdd_from_list = sc.parallelize([1,2,3,4])

squared = rdd_from_list.map(lambda x:x*x)
sum_all = rdd_from_list.map(lambda x:x*x).reduce(lambda x, y:x+y)

print(squared.collect())
print(sum_all)
