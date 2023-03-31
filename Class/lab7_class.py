from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local[*]").setAppName("lab7")
sc = SparkContext(conf = conf)

#1. load rdd from external file
lines = sc.textFile("peterpan.txt")

#2. transformation
#without nltk
#words = lines.flatMap(lambda x:x.lower().split(" "))

#with nltk
import nltk
from nltk.corpus import stopwords
stopword_list = set(stopwords.words("english"))

def ProcessText(text):
    tokens = nltk.word_tokenize(text)
    remove_punct = [word.lower() for word in tokens if word.isalpha()]
    remove_stop_words = [word for word in remove_punct if not word in stopword_list]
    return remove_stop_words

words = lines.flatMap(lambda x:ProcessText(x))

#3. create the key-value pairs
inter_rdd = words.map(lambda x:(x,1))

#4. final output word frequency
result = inter_rdd.reduceByKey(lambda x,y:x+y)
result_collect = result.collect()

print(sorted(result_collect, key = lambda x:x[1],reverse = True)[:100])
