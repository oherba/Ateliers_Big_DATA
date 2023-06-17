from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, count

# Initialize SparkSession
spark = SparkSession.builder.appName("NewsApp").getOrCreate()

# Create a streaming DataFrame from the socket connection
lines = spark.readStream.format("socket").option("host", "localhost").option("port", 4523).load()

# Split the lines into words
words = lines.select(explode(split(lines.value, "\n")).alias("desc"))

# Process each batch and print the results

query = words.writeStream.outputMode("append").format("console").start()

# Wait for the streaming query to finish
query.awaitTermination()




