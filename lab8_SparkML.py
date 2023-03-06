#Create DataFrame
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml import Pipeline
from pyspark.ml.feature import Tokenizer, HashingTF, IDF 
from pyspark.ml.classification import LogisticRegression

sc = SparkContext("local")
spark = SparkSession(sc)

#1. Create training data, to a DataFrame, id, text, label as spam-0/nonspam-1
training = spark.createDataFrame([
	(0,"a b c d e spark",3.0),
	(1,"b d",0.0),
	(2,"spark f g h",1.0),
	(3,"hadoop mapreduce",0.0)
	],["id","text","label"])

#2 ML pipeline
tokenizer = Tokenizer(inputCol = "text", outputCol = "words") 
TF = HashingTF(inputCol = tokenizer.getOutputCol(), outputCol = "tfFeatures")
idf = IDF(inputCol = "tfFeatures",outputCol = "features")
lr = LogisticRegression(maxIter = 10, regParam = 0.001)

pipeline = Pipeline(stages = [tokenizer,TF,idf,lr])
model = pipeline.fit(training)

#3. Test data set, unlabeled (id, text)
test = spark.createDataFrame([
	(4, "a b c d e"),
	(5, "l m n"),
	(6, "spark hadoop spark"),
	(7,"apache hadoop")
	],["id","text"])

#4. Make Prediction
prediction = model.transform(test)

selected = prediction.select("id","text","probability","prediction")
for row in selected.collect():
	id, text, prob, prediction = row
	print("(%d, %s) --> prob = %s, prediction = %f" %(id, text, str(prob),prediction))
