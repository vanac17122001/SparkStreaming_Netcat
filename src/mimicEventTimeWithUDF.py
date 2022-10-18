from xmlrpc.client import DateTime
from pyspark.sql.types import * 
from pyspark.sql import SparkSession

from pyspark.sql.functions import udf 
import time 
import datetime


if __name__ == "__main__" :
    
    sparkSession = SparkSession.builder.master("local")\
                               .appName("SparkStreamingAddTimestamp")\
                               .getOrCreate()
                               
    
    sparkSession.sparkContext.setLogLevel("ERROR")


    schema = StructType([StructField("category", StringType(), True),\
                         StructField("on twitter since", StringType(), True),\
                         StructField("twitter handle", StringType(), True),\
                         StructField("profile url", StringType(), True),\
                         StructField("followers", StringType(), True),\
                         StructField("following", StringType(), True),\
                         StructField("profile location", StringType(), True),\
                         StructField("profile lat/lon", StringType(), True),\
                         StructField("profile description", StringType(), True)])

    fileStreamDF = sparkSession.readStream\
                               .option("header", "true")\
                               .option("maxFilesPerTrigger", 2)\
                               .schema(schema)\
                               .csv("../datasets/dropfolder")

    fileStreamDF=fileStreamDF.withColumnRenamed("twitter handle","twitter_handle")\
                             .withColumnRenamed("profile location","profile_location")


    def add_timestamp ():
        ts = time.time()
        timestamp = datetime.datetime.fromtimestamp(ts).strftime('%Y-%m-%d %H:%M:%S')
        return timestamp
    
    add_timestamp_udf = udf(add_timestamp, StringType())
    
    fileStreamWithTS = fileStreamDF.withColumn("timestamp", add_timestamp_udf())
    
    
    trimmedDF = fileStreamWithTS.select("category",
                                        "twitter_handle",
                                        'followers',
                                        "timestamp")
    
    # Write the trimmed DF to console
    query = trimmedDF.writeStream\
                        .outputMode('append')\
                        .format('console')\
                        .option("truncate","false")\
                        .start()\
                        .awaitTermination()