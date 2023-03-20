from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark import SparkConf

#from pyspark.ml.feature import Tokenizer, HashingTF, IDF 
#from pyspark.ml.classification import LogisticRegression
#from pyspark.ml import Pipeline
from pyspark.sql.functions import col

conf = SparkConf().setMaster("local[*]").set("spark.executer.memory", "2g")

sc = SparkContext(conf=conf)
spark = SparkSession(sc).builder.getOrCreate()
spark.sparkContext.setLogLevel("ERROR")


#"review_id","user_id","business_id","stars","date","text","useful","funny","cool"

#1. Clean the dataset

df = spark.read.format("csv").option("header", "true").option("multiline","true").load("yelp-100.csv")
df.printSchema()
df.show()
df = df.withColumn("label", df["stars"].cast("double"))

df = df.select('text', 'label')

df = df.filter(df.label.isin(1.0,5.0))

df.write.csv("yelp-cleaned")
