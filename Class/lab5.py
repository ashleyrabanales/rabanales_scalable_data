#%%
from pyspark import SparkConf, SparkContext

#SparkContext
conf = SparkConf().setMaster("local").setAppName("squared")
sc = SparkContext(conf=conf)

rdd_from_list = sc.parallelize([1,2,3,4])
#

squared = rdd_from_list.map(lambda x:x*x)
sum_all = rdd_from_list.map(lambda x:x*x).map(lambda x:x*2).reduce(lambda x, y:x+y)

print(squared.collect())
print(sum_all)

#%%
#installing spark
#!pip install pyspark
# %%
