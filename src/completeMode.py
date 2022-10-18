from pyspark.sql.types import *
from pyspark.sql import SparkSession


if __name__ == "__main__" :
    
    # Set your local host to be the master node 
    # Set the appName
    # Join session for app if it exists, else create a new one
    SparkSession = SparkSession.builder.master("local")\
        .appName("SparkStreamingCompleteMode")\
            .getOrCreate()
    
    SparkSession.sparkContext.setLogLevel("ERROR")
    
    
    schema = StructType([StructField("category", StringType(), True),\
        StructField("on twitter since", StringType(), True),\
            StructField("twitter handle", StringType(), True),\
                StructField("profile url", StringType(), True),\
                    StructField("followers", StringType(), True),\
                        StructField("following", StringType(), True),\
                            StructField("profile location", StringType(), True),\
                        StructField("profile lat/lon", StringType(), True),\
                            StructField("profile description", StringType(), True)])
    
    
    # Read stream intpt a dataframe
    # Set header row, schema, location csv files
    # maxFilesPerTrigger sét the number of new files to be considered in each trigger
    fileStreamDF = SparkSession.readStream\
        .option("header","true")\
        .option("maxFilesPerTrigger",1)\
            .schema(schema)\
                .csv("../datasets/dropfolder")        
    
    
    recordsPerCategory = fileStreamDF.groupBy("category")\
       .count()\
           .orderBy("count",ascending=False)
           
    
    query = recordsPerCategory.writeStream\
        .outputMode("complete")\
        .format("console")\
        .option("truncate","false")\
        .option("numRows",10)\
        .start()\
        .awaitTermination()
       
    