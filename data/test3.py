from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split
import sys
# docker exec -it dockerspark_master_1 ./bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.2.0 /tmp/data/test3.py $DOCKER_MAIN_IP
#in_ip = "192.168.2.168:2181"
if __name__ == '__main__':
    if len(sys.argv) != 2:
        print("Usage: kafka_wordcount.py <ip>", file=sys.stderr)
        exit(-1)
    ip = sys.argv[1]
    in_ip = "%s:32771,%s:32772,%s:32773"%(ip,ip,ip)
    in_topic = "test"

    #out_ip = "192.168.2.168:2182"
    out_ip = "%s:32774,%s:32775,%s:32776"%(ip,ip,ip)
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