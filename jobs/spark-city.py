from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col 
from pyspark.sql import DataFrame
from pyspark.sql.types import StructType,StructField, StringType, TimestampType, DoubleType,IntegerType
from config import configuration

def main():
    spark = SparkSession.builder.appName("SmartCityStreaming") \
                     .config("spark.jars.packages", 
                             "org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,"
                             "org.apache.hadoop:hadoop-aws:3.3.1,"
                             "com.amazonaws:aws-java-sdk:1.11.901") \
                    .config("spark.hadoop.fs.s3a.impl","org.apache.hadoop.fs.s3a.S3AFileSystem")\
                    .config("spark.hadoop.fs.s3a.access.key",configuration.get("AWS_ACCESS_KEY"))\
                    .config("spark.hadoop.fs.s3a.secret.key",configuration.get("AWS_SECRET_KEY"))\
                    .config("spark.hadoop.fs.s3a.aws.credentials.provider","org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")\
                    .getOrCreate()

    # Adjust the log level to minimize the console output on executors
    spark.sparkContext.setLogLevel('WARN')
              
    #VechileSchema
    vechileSchema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("make", StringType(), True),
            StructField("model", StringType(), True),
            StructField("year", IntegerType(), True),
            StructField("fuelType", StringType(), True)
    ])
    # GpsSchema
    gpsSchema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("speed", DoubleType(), True),
            StructField("direction", StringType(), True),
            StructField("vechileType", StringType(), True)
    ])                     

    # TrafficSchema
    trafficSchema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("cameraId", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("snapshot", StringType(), True),
    ])                     
    #WeatherSchema
    weatherSchema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("location", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("temperature", DoubleType(), True),
            StructField("weatherCondition", StringType(), True),
            StructField("precipitation", DoubleType(), True),
            StructField("windSpeed", DoubleType(), True),
            StructField("humidity", IntegerType(), True),
            StructField("airQualityIndex", DoubleType(), True)
    ])

    # Emergencychema
    emergencySchema = StructType([
            StructField("id", StringType(), True),
            StructField("deviceId", StringType(), True),
            StructField("incidentId", StringType(), True),
            StructField("type", StringType(), True),
            StructField("timestamp", TimestampType(), True),
            StructField("location", StringType(), True),
            StructField("status", StringType(), True),
            StructField("description", StringType(), True),
    ])  

    def read_kafka_topic(topic, schema):
        return (
            spark.readStream
                 .format('kafka')
                 .option('kafka.bootstrap.servers','broker:29092')
                 .option('subscribe',topic)
                 .option('startingOffsets','earliest')
                 .load()
                 .selectExpr('CAST(value as STRING)')
                 .select(from_json(col('value'), schema).alias('data'))
                 .select('data.*')
                 .withWatermark('timestamp','2 minutes')
        )
    
    def streamWriter(input: DataFrame, checkpoint, output):
        return (
            input.writeStream
              .format('parquet')
              .option('checkpointLocation','checkpointFolder')
              .option('path',output)
              .outputMode('append')
              .start()
        )

    vechileDF = read_kafka_topic('vechile_data', vechileSchema).alias('vechile')
    gpsDF = read_kafka_topic('gps_data', gpsSchema).alias('gps')
    trafficDF = read_kafka_topic('traffic_data', trafficSchema).alias('traffic')
    weatherDF = read_kafka_topic('weather_data', weatherSchema).alias('weather')
    emergencyDF = read_kafka_topic('emergency_data', emergencySchema).alias('emergency')

    query1 = (
        vechileDF
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
    )
    query2 = (
        gpsDF
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
    )
    query3 = (
        trafficDF
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
    )
    query4 = (
        weatherDF
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
    )
    query5 = (
        emergencyDF
          .writeStream
          .format("console")
          .outputMode("append")
          .start()
    )
    query5.awaitTermination()


    # query1 = streamWriter(vechileDF, 's3a://spark-streaming-data/checkpoints/vechile_data','s3a://spark-streaming-data/vechile_data')
    # query2 = streamWriter(gpsDF, 's3a://spark-streaming-data/checkpoints/gps_data','s3a://spark-streaming-data/gps_data')
    # query3 = streamWriter(trafficDF, 's3a://spark-streaming-data/checkpoints/traffic_data','s3a://spark-streaming-data/traffic_data')
    # query4 = streamWriter(weatherDF, 's3a://spark-streaming-data/checkpoints/weather_data','s3a://spark-streaming-data/weather_data')
    # query5 = streamWriter(emergencyDF, 's3a://spark-streaming-data/checkpoints/emergency_data','s3a://spark-streaming-data/emergency_data')

    # query5.awaitTermination()

if __name__ == '__main__':
    main()



## Maven repostirory ==> SQL Kafka 
## kafka 0.10+ source for structure streaming : 3.5.0
# groupId,  artificact:id :verion; 

### Pakage to connect maven to AWS 
## Apacjage hadoop aamaon aws 



#### clear all the data from topic 
## delete all the optics first , before running 
## kafka-topics --delete --topic emeer --bootstrap-server broker:290092

## First Run 

#docer exec -it smart

## to check the cluster 
## localhost: 9090 

### Submit the jobs 
# docker exec -it smartcity-spark-master-1 spark-submit \ --master spark://spark-master:7077 \ --packages org.apache.spark:spark-sql-kafka-0_10_2.12:3.5.0,org.apache.hadoop:hadoop-aws:3.3.1,com.amazonaws:aws-java-sdk:1.11.469 jobs/spark-city.py

## start producing dataa as well 
#python jobs/main.py 