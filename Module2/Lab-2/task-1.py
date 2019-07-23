import os
from pyspark import SparkContext

os.environ["SPARK_HOME"] = "C:\\spark\\spark-2.4.1-bin-hadoop2.7"
os.environ["HADOOP_HOME"] = "C:\\winutils\\"

def map(Text):
    Text = Text.split(" ")
    profile = Text[0]
    friends = Text[1]
    key = []
    for friend in friends:
        key.append((''.join(sorted(profile + friend)), friends.replace(friend, "")))
    return key
def reduce(key, value):
    reducer = ''
    for friend in key:
        if friend in value:
            reducer += friend
    return reducer
if __name__ == "__main__":
    sc = SparkContext.getOrCreate()
    Lines = sc.textFile("facebook_combined.txt", 1)
    Line = Lines.flatMap(map)
    fbCommonfriends = Line.reduceByKey(reduce)
    fbCommonfriends.coalesce(1).saveAsTextFile("fbCommonFriendsOutput")
    sc.stop()