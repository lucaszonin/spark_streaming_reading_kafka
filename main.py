from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":

    spark = SparkSession.builder.appName("Kafka + Spark Streaming Test").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    tabela_big_query = "datasetSparkStargate.tabela-kafka-spark-stargate-v2"
    spark.conf.set("temporaryGcsBucket","bucket-spark31-stargate-v2")

    df = (
        spark.readStream.format("kafka")
        .option(
            "kafka.bootstrap.servers",
            "10.128.15.203:9092,10.128.15.206:9092,10.128.15.207:9092",
        )
        .option("subscribe", "google-analytics")
        .option("group.id", "stargate-spark-group")
        .load()
    )
    df.printSchema()
    df.selectExpr("CAST(key AS STRING) as key", "CAST(value AS STRING) as value")
    
    df2 = df.withColumn("key", df.key.cast("string")).withColumn(
        "value", df.value.cast("string")
    )
    df2.printSchema()

    schema_bigquery = StructType([
        StructField("hitType", StringType(), True),
        StructField("page", StringType(), True),    
        StructField("clientId", StringType(), True),
        StructField("eventCategory", StringType(), True),
        StructField("eventAction", StringType(), True),
        StructField("eventLabel", StringType(), True),
        StructField("utmSource", StringType(), True),
        StructField("utmMedium", StringType(), True),
        StructField("utmCampaign", StringType(), True),
        StructField("timestampGTM", StringType(), True),
    ])

    dataframe = df2.withColumn("dataLayer", from_json(col("value"), schema_bigquery)).select(col('dataLayer.*'), col('key'), col("topic"), col('partition'), col('offset'), col('timestamp'))

    query = (
        dataframe.writeStream.format("console").outputMode("append")
        .trigger(processingTime="60 second")
        .start()
    )

    query = (
        dataframe.writeStream 
        .format("bigquery") 
        .option("checkpointLocation", "gs://bucket-spark31-stargate-v2/checkpoints/") 
        .option("table", tabela_big_query)
        .trigger(processingTime="60 second")
        .start()
    )

    query.awaitTermination()


# Command to send the job to Dataproc Cluster in GCP
# gcloud dataproc jobs submit pyspark --cluster cluster-spark31-stargate-v2 gs://bucket-spark31-stargate-v2/main.py --region us-central1 --properties spark.jars.packages=org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.3 --jars gs://spark-lib/bigquery/spark-bigquery-with-dependencies_2.12-0.24.2.jar
