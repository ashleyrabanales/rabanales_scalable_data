# -*- coding: utf-8 -*-
"""Top Words -2.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1IrWn8KiEaLBB9Jk34pFLQcsvb6EZzkHP
"""

pip install stopwords

import nltk
nltk.download('stopwords')

pip install pyspark

import re
import nltk
from pyspark import SparkConf, SparkContext

#use similar codes from length 1 but adding stopwords
from nltk.corpus import stopwords
stopword_list = set(stopwords.words("english"))

conf = SparkConf().setMaster("local[*]").setAppName("WordCount")
sc = SparkContext(conf=conf)


text = sc.textFile("Amazon_Comments.csv") 
text_1 = text.map(lambda x: x.split("^"))

text_clean = text_1.map(lambda x: (x[6], re.sub('\W+',' ', x[5]).strip().lower()))

#making a loop to remove certain words over csv
def ProcessText(text,stopword_list):
	tokens = nltk.word_tokenize(text)
	remove_stop_words = [word for word in tokens if not word in stopword_list]	
	return remove_stop_words

nltk.download('punkt')

#had a bit issues with packages downloads and code not running due to that
rdd = text_clean.filter(lambda x:x[0]=="1.00").map(lambda x:x[1])
token = rdd.flatMap(lambda x:ProcessText(x,stopword_list))
inter_rdd = token.map(lambda x:(x,1))

result = inter_rdd.reduceByKey(lambda x,y:x+y)
print("1 star rating:" + str(sorted(result.collect(),key = lambda x:x[1],reverse=True)[:10]))