from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.ml.feature import Tokenizer, HashingTF, IDF 
from pyspark.ml.classification import LogisticRegression
from pyspark.ml import Pipeline
from pyspark.sql.functions import col


sc = SparkContext("local[*]")
spark = SparkSession(sc)

#"review_id","user_id","business_id","stars","date","text","useful","funny","cool"

#1. Clean the dataset

df = spark.read.csv( "yelp-dataset/yelp_review.csv", header=True)
df.printSchema()
df.show()
#df = df.withColumnRenamed("stars", "label")
df = df.withColumn("label", df["stars"].cast("double"))
#df = df.where(col("label").isNotNull())
df = df.dropna(subset=['label', 'text', 'funny', 'cool',"useful"])
df.show()
df.printSchema()

df = df.select('text', 'label')
df.show()

df = df.filter(df.label.isin(2.0,4.0))
df.show()

df = df.limit(1000)
df.show()

#2 ML pipeline
tokenizer = Tokenizer(inputCol = "text", outputCol = "words") 
TF = HashingTF(inputCol = tokenizer.getOutputCol(), outputCol = "tfFeatures")
idf = IDF(inputCol = "tfFeatures",outputCol = "features")
lr = LogisticRegression(maxIter = 10, regParam = 0.001)

pipeline = Pipeline(stages = [tokenizer,TF,idf,lr])
model = pipeline.fit(df)

#3. Test data set, unlabeled (id, text)
test = spark.createDataFrame([
	(4, "Worst restaurant ever"),
	(5, "This restaurant is absolutely the best"),
	(6, "best hotel ever"),
	(7, "I love this hotel")
	],["id","text"])

#4. Make Prediction
prediction = model.transform(test)

selected = prediction.select("id","text","probability","prediction")
for row in selected.collect():
	id, text, prob, prediction = row
	print("(%d, %s) --> prediction = %f" %(id, text, prediction))
