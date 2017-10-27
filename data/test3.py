from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import sys
# docker exec -it dockerspark_master_1 ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 /tmp/data/test3.py $DOCKER_MAIN_IP
#in_ip = "192.168.2.168:2181"

# DOCKER_MAIN_IP
if __name__ == '__main__':
    if len(sys.argv) != 1:
        print("Usage: kafka_wordcount.py <ip>", file=sys.stderr)
        exit(-1)
    ip = sys.argv[0]

    in_ip = "%s:32781,%s:32782,%s:32783"%(ip,ip,ip)
    in_topic = "in"

    #out_ip = "192.168.2.168:2182"
    out_ip = "%s:32784,%s:32785,%s:32786"%(ip,ip,ip)
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