from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import re

if __name__ == "__main__":
	sc = SparkContext("local[2]",appName = "NetworkWordCount")
	sc.setLogLevel("ERROR")
	ssc = StreamingContext(sc,10)
	
	#Create DStream from data source
	lines = ssc.socketTextStream("localhost",9999)
	
	#Transformations and actions on DStream
        #Split according to punctuation
	counts = lines.flatMap(lambda line:re.findall(r"[\w']+|[.,!?;]", line))\
		 .map(lambda word: (word,1)) \
		 .reduceByKey(lambda a,b:a+b)
	counts.pprint()
	
	#Start listening to the server
	ssc.start()
	ssc.awaitTermination()
