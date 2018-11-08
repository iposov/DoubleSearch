from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession
from pymongo import MongoClient
import urllib.parse

import os

# os.environ["SPARK_HOME"] = "/Applications/spark/spark-2.3.1-bin-hadoop2.7"
os.environ["PYSPARK_PYTHON"] = "/Users/martikvm/anaconda3/envs/py35/bin/python"
# os.environ["PYSPARK_DRIVER_PYTHON"] = "/Users/martikvm/anaconda3/envs/py35/bin/python"

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

# df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
# df.printSchema()
# df.select("text").write.text("/Users/martikvm/PycharmProjects/DoubleSearch/twitter/all_tweets_from_mongo.txt")

# spark = SparkSession \
#    .builder \
#    .master("local") \
#    .appName("twitter") \
#    .getOrCreate()
#
# df = spark.read.text("/Users/martikvm/PycharmProjects/DoubleSearch/twitter/tweets_sample.txt")
# df.printSchema()

############## Очистка твитов
#import re
#
#with open('/Users/martikvm/PycharmProjects/DoubleSearch/twitter/normalizedTweetsWithoutLinks.txt', 'a') as output, open("/Users/martikvm/PycharmProjects/DoubleSearch/twitter/all_tweets_from_mongo/part-00002-8f268b47-3135-4f42-921c-729e6860189b-c000.txt", "r") as input:
#    print("Something")
#    for tweet in input:
#        oneTweet = re.sub("http(\S)+", "[link]", tweet)
#        oneTweet = re.sub("@(\S)+", "[user]", oneTweet)
#        output.write('\n' + oneTweet + '\n')


from pyspark.ml import Pipeline
from pyspark.ml.feature import MinHashLSH, NGram, Tokenizer, CountVectorizer
from pyspark.sql.functions import col

df1 = spark.read.text("/Users/martikvm/PycharmProjects/DoubleSearch/twitter/normalizedTweetsWithoutLinks.txt").limit(500)
#df1 = spark.read.text("/Users/martikvm/PycharmProjects/DoubleSearch/twitter/tweets_sample.txt")
tokenizer = Tokenizer(inputCol="value", outputCol="words")
ngram = NGram(n=1, inputCol="words", outputCol="ngrams")
cv = CountVectorizer(inputCol="ngrams", outputCol="features")
pipeline = Pipeline(stages=[tokenizer, ngram, cv])
model = pipeline.fit(df1)
df2 = model.transform(df1)
df2.show()

from pyspark.ml.linalg import Vectors, SparseVector
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

def getsparsesize(v):
    return v.values.size


getsize_udf = udf(getsparsesize, IntegerType())
df2_with_lengths = df2.select("value", "features", getsize_udf("features").alias("vec_size"))
df2_with_lengths.show()

df2NotNull = df2_with_lengths.filter(getsize_udf(df2["features"]) != 0)

mh = MinHashLSH(inputCol="features", outputCol="hashes", numHashTables=128)
model2 = mh.fit(df2)
transformed_df2 = model2.transform(df2NotNull)
transformed_df2.show()

########## Этот блок зависает
#spark.sql("set spark.sql.shuffle.partitions=3");

df_pairs = model2.approxSimilarityJoin(transformed_df2, transformed_df2, 0.4, distCol="JaccardDistance")

for row in df_pairs.limit(100).collect():
    print(row.datasetA.value)
    print(row.datasetB.value)
    print(row.JaccardDistance)
    print()
#    .select(col("datasetA.value").alias("idA"),
#            col("datasetB.value").alias("idB"),
#            col("JaccardDistance")).show()
#