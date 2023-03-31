#!/usr/bin/env python
# -*- coding: utf-8 -*-

# SparkDemo.py
# ﻿This code is copyright (c) 2017 by Laurent Weichberger.
# Authors: Laurent Weichberger, from Hortonworks and,
# from RAND Corp: James Liu, Russell Hanson, Scot Hickey,
# Angel Martinez, Asa Wilks, & Sascha Ishikawa
# This script does use Apache Spark. Enjoy...
# This code was designed to be run as: spark-submit SparkDemo.py

 
import time
import json
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
import re

 
# SparkContext(“local[1]”) would not work with Streaming bc 2 threads are required
sc = SparkContext("local[2]", "Twitter Demo")
ssc = StreamingContext(sc, 10) #10 is the batch interval in seconds

sc.setLogLevel("ERROR")

IP = "localhost"
Port = 5555
lines = ssc.socketTextStream(IP, Port)
 
# When your DStream in Spark receives data, it creates an RDD every batch interval.
# We use time.time() to make sure there is always a newly created directory, otherwise
# it will throw an Exception 

#split according to puncation

counts = lines.flatMap(lambda line: re.findall(r"[\w']+|[.,!?;]", line))\
                  .map(lambda word: (word, 1))\
                  .reduceByKey(lambda a, b: a+b)

#counts.pprint()
counts.saveAsTextFiles("./tweets/%f" % time.time())
#rdd.saveAsTextFile("ouput")
        
# You must start the Spark StreamingContext, and await process termination…
ssc.start()
ssc.awaitTermination()
