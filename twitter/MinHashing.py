from pyspark.ml.feature import Word2Vec
from pyspark.sql import SparkSession
from pymongo import MongoClient
import urllib.parse
import pyspark.sql.functions as sqlf
from pyspark.ml import Pipeline
from pyspark.ml.feature import MinHashLSH, NGram, Tokenizer, CountVectorizer
from pyspark.sql.functions import col
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType

import os
import sys

all_saved_tweets = '/Users/martikvm/PycharmProjects/DoubleSearch/twitter/all_tweets_from_mongo_2019'
all_saved_tweets_file = '/Users/martikvm/PycharmProjects/DoubleSearch/twitter/normalizedTweetsWithoutLinks_2019.txt'
normalized_file = '/Users/martikvm/PycharmProjects/DoubleSearch/twitter/normalizedTweetsWithoutLinks_2019_test.txt'
results_file = '/Users/martikvm/PycharmProjects/DoubleSearch/twitter/resultsTweetsAlike_2019_test2.txt'


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
            "@doublesearchintwitter-m3qge.mongodb.net/twitter_march2019.tweets") \
    .getOrCreate()

#df = spark.read.format("com.mongodb.spark.sql.DefaultSource").load()
#df.printSchema()
#df.select("text").write.text(all_saved_tweets)

############## Очистка твитов
import re

#with open(normalized_file, 'a') as output, open(all_saved_tweets_file, "r") as input:
#    print("Something")
#    for tweet in input:
#        oneTweet = re.sub("http(\S)+", "[link]", tweet)
#        oneTweet = re.sub("@(\S)+", "[user]", oneTweet)
#        output.write('\n' + oneTweet + '\n')

df1 = spark.read.text(normalized_file)
tokenizer = Tokenizer(inputCol="value", outputCol="words")
ngram = NGram(n=1, inputCol="words", outputCol="ngrams")
cv = CountVectorizer(inputCol="ngrams", outputCol="features")
pipeline = Pipeline(stages=[tokenizer, ngram, cv])
model = pipeline.fit(df1)
df2 = model.transform(df1)
df2.show()


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

edges = []
for k in range(0, transformed_df2.count()):
    edges.append(k)
print(edges)

def getHashColumns(df0, x):
    sum_of_hashes = 0
    for y in range(x, x + 4):
        sum_of_hashes += int(df0[y][0])
    return sum_of_hashes


gethashsums_udf = udf(getHashColumns)

with open(results_file, 'w') as outf:
    for k in range(0, 128, 4):
        print("k = ", k)
        outf.write("============================\nk = " + str(k) + "\n============================\n")

        df3 = transformed_df2.select("value", "id", gethashsums_udf("hashes", sqlf.lit(k)).alias("hashes03")).groupBy(
            "hashes03").agg(sqlf.count('*').alias("num_tweets"), sqlf.collect_list("value").alias("tweets_texts"),
                            sqlf.collect_list("id").alias("ids")).filter(
            col("num_tweets") > 1)
        df3.show()

        new_colour = 1

        for row in df3.select("ids").collect():
            flag = True
            print(row)
            for id in row.ids:
                to_change = []
                if (flag):
                    new_colour = id
                    flag = False
                    if (edges[id] != id):
                        new_colour = edges[id]
                else:
                    to_change.append(edges[id])
                edges[id] = new_colour
                for i in range(0, len(edges)):
                    if edges[i] == id:
                        edges[i] = new_colour
                for k in to_change:
                    for i in range(0, len(edges)):
                        if (edges[i] == k):
                            edges[i] = new_colour

        print(edges)

        print('transformed')
        print()

        for row in df3.collect():
            for oneTweet in row.tweets_texts:
                outf.write(oneTweet + '\n')
            outf.write(str(row.num_tweets))
            outf.write('\n\n')

