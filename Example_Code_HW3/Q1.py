import re
from pyspark import SparkConf, SparkContext

conf  = SparkConf().setMaster("local[*]").setAppName("length") 
sc = SparkContext(conf=conf)

text = sc.textFile("Amazon_Comments.csv")
text_1 = text.map(lambda x: x.split("^"))

text_clean = text_1.map(lambda x: (x[6],len(re.sub('\W+', ' ', x[5]).strip().split(" "))))

t_map = text_clean.mapValues(lambda x: (x, 1))
t_reduce = t_map.reduceByKey(lambda x,y: (x[0]+y[0], x[1]+y[1]))

result = sorted(t_reduce.collect())
# print out needed output 
for i in result: 
	print("Rating: "+ i[0][:1] + "; average length of review: " + str(i[1][0]/i[1][1]))
	
 
 

 
