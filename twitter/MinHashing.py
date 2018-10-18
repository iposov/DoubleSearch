from pyspark.sql import SparkSession
from pymongo import MongoClient
import urllib.parse
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinHashLSH, NGram, Tokenizer, CountVectorizer
from pyspark.sql.functions import col

credPath = '/Users/martikvm/PycharmProjects/DoubleSearch/twitter/credentialsMongo.txt'
credFile = open(credPath, 'r')
loginMA = urllib.parse.quote_plus(credFile.readline().strip())
passwordMA = urllib.parse.quote_plus(credFile.readline()).strip()
client = MongoClient("mongodb+srv://" +
                     loginMA + ":" +
                     passwordMA +
                     "@doublesearchintwitter-m3qge.mongodb.net")
db = client.twitter_database
tweets = db.tweets

allTweets = tweets.find(no_cursor_timeout=True).sort("text", 1)
keepAllTweets = []

spark = SparkSession \
    .builder \
    .master("local") \
    .appName("twitter") \
    .config('spark.jars.packages', 'org.mongodb.spark:mongo-spark-connector_2.11:2.3.0') \
    .config("spark.mongodb.input.uri", "mongodb+srv://" +
            loginMA + ":" +
            passwordMA +
            "@doublesearchintwitter-m3qge.mongodb.net/twitter_database.tweets") \
    .getOrCreate()

df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
df.printSchema()

tokenizer = Tokenizer(inputCol="text", outputCol="words")
ngram = NGram(n=3, inputCol="words", outputCol="ngrams")
cv = CountVectorizer(inputCol="words", outputCol="features")
mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=5)
pipeline = Pipeline(stages=[tokenizer, ngram, cv, mh])
model = pipeline.fit(df)
model.transform(df).select(col("text"), col("hashes")).show(truncate=False)
