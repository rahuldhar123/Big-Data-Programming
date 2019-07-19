import os
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from collections import namedtuple

os.environ["SPARK_HOME"] = "C:\spark\spark-2.4.2-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils\\"


if __name__ == "__main__":
    sc = SparkContext(appName="SparkStreaming")
    ssc = StreamingContext(sc, 5)
    lines = ssc.socketTextStream("localhost", 9999)
    fields = ("Text", "count")
    Tweet = namedtuple('Hastags',fields)
    wordcounts = lines.flatMap(lambda text: text.split(" ")).map(lambda x: (x, 1)) \
        .reduceByKey(lambda a, b: a + b).map(lambda rec: Tweet(rec[0], rec[1])).encode()
    wordcounts.pprint()
    ssc.start()
    ssc.awaitTermination()