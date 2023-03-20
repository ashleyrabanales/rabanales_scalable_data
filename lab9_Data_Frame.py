#1. create SparkSession

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import avg

sc = SparkContext("local")
spark = SparkSession(sc)

#1. Create a DataFrame
dataDF = spark.createDataFrame([("Jim",20),("Anne",31),("Jim",30)],["name","age"])
#2. DataFrame operations
result2 = dataDF.groupBy("name").agg(avg("age"))

dataDF.show()
print(result2.collect())

dataRDD = sc.parallelize([("Jim",20),("Anne",31),("Jim",30)])
result1 = dataRDD.map(lambda x:(x[0],(x[1],1))).reduceByKey(lambda x,y:(x[0]+y[0],x[1]+y[1])).map(lambda x:(x[0],x[1][0]/x[1][1]))
print(result1.collect())
