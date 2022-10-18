from pyspark.sql.types import *
from pyspark.sql import SparkSession


if __name__ == "__main__" :

    sparkSesion = SparkSession.builder.master("local")\
        .appName("SparkStreamingAppendMode")\
        .getOrCreate()

    
    sparkSesion.sparkContext.setLogLevel("ERROR")


    schema = StructType([StructField("category", StringType(),True),\
        StructField("on twitter since", StringType(),True),\
        StructField("twitter handle", StringType(),True),\
        StructField("profile url", StringType(),True),\
        StructField("followers", StringType(),True),\
        StructField("following", StringType(),True),\
        StructField("profile location", StringType(),True),\
        StructField("profile lat/lon", StringType(),True),\
        StructField("profile description", StringType(),True)])
    

    fileStreamDF = sparkSesion.readStream\
        .option("header","true")\
        .option("maxFilesPerTrigger",4)\
        .schema(schema)\
        .csv("../datasets/dropfolder")
    
    fileStreamDF.createOrReplaceTempView("disaster_accident_crime_accounts")


    categoryDF = sparkSesion.sql("Select category, following\
                                from disaster_accident_crime_accounts\
                                where followers > '150000'")

    from pyspark.sql.functions import format_number
    from pyspark.sql.functions import col

    followingPerCategory = categoryDF.groupBy("category")\
                                    .agg({"following":"sum"})\
                                    .withColumnRenamed("sum(following)", "total_following")\
                                    .orderBy("total_following", ascending=False)\
                                    .withColumn("total_following", format_number(col("total_following"),0))


    query = followingPerCategory.writeStream\
                .outputMode("complete")\
                .format("console")\
                .option("truncate", "false")\
                .start()\
                .awaitTermination()