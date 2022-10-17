import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split


# Check that correct number of args have been passed as input
if __name__ == "__main__":
    
    if len(sys.argv) != 3:
        print("Usage: spark-submit <src-file> <hostname> <port>", file=sys.stderr)
        exit(-1)


    # Extract host and port from args    
    host = sys.argv[1]
    port = int(sys.argv[2])

    
    # Set the app name when creating a Spark session
    # If a Spark session is already created for the app, use that. 
    # Else create a new session for that app
    spark = SparkSession\
        .builder\
        .appName("SparkLetterCount")\
        .getOrCreate()


    # Set log level. Use ERROR to reduce the amount of output seen
    spark.sparkContext.setLogLevel("ERROR")


    # Create DataFrame representing the stream of input data from connection to host:port
    # We're reading from the socket on the port where netcat is listening
    data = spark\
            .readStream\
            .format('socket')\
            .option('host', host)\
            .option('port', port)\
            .load()


    # Split the data into alphabets
    # Explode turns each item in an array into a separate row
    # Alias sets the name of the column for the alphabets
    # The result - each word of input is a row in a table with one column named "spark_letters"
    alphabets = data.select(
                            explode(
                                split(data.value,'')
                            ).alias('spark_letters')
    )


    sparkLetters = ['s','p','a','r','k']
    # Generate running letter count
    # Count all occurences of the "spark" letters in the current batch
    sparkLettersCounts = alphabets.filter(alphabets['spark_letters'].\
                            isin(sparkLetters)).groupBy('spark_letters').count()

    


    # Start running the query that prints the running counts to the console
    # Running in "update" mode ensures that any operation uses ALL count data 
    # - from previous and current batch
    # The alphabets which are ouput are only from the current batch, 
    # but the counts include all previous batches
    # The call to format sets where the stream is written to
    query = sparkLettersCounts.writeStream\
                              .outputMode('update')\
                              .format('console')\
                              .start()

    query.awaitTermination()






