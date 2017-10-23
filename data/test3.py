from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
# docker exec -it dockerspark_master_1 ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 /tmp/data/test.py
#in_ip = "192.168.2.168:2181"
in_ip = "192.168.2.168:32768,192.168.2.168:32769,192.168.2.168:32770"
in_topic = "test"

#out_ip = "192.168.2.168:2182"
out_ip = "192.168.2.168:32773,192.168.2.168:32772,192.168.2.168:32771"
out_topic = "out"

spark = SparkSession.builder.appName("StructuredNetInOut").getOrCreate()
# http://kafka.apache.org/documentation.html#newconsumerconfigs
df = spark \
    .readStream.format("kafka") \
    .option("kafka.bootstrap.servers", in_ip) \
    .option("subscribe", in_topic) \
    .load()
#df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")    

#df.printSchema()

ds = df \
  .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
  .writeStream \
  .format("kafka") \
  .option("kafka.bootstrap.servers",out_ip) \
  .option("checkpointLocation", "/tmp/data/") \
  .option("topic", out_topic) \
  .start() \
  .awaitTermination()


# https://spark.apache.org/docs/2.2.0/structured-streaming-programming-guide.html

csvDF = df \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream \
    .format("csv") \
    .option("path","/tmp/data/aha.csv") \
    .option("checkpointLocation", "/tmp/data/") \
    .start() \
    .awaitTermination()