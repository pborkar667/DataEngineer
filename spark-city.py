from pyspark.sql import SparkSession # type: ignore
from config import configuration 

def main():
    Spark = SparkSession.builder.appName("SmartCityStreaming") \
    .config("spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.13:3.5.0," 
            "org.apache.hadoop:hadoop-aws:3.3.1," 
            "com.amazonaws:aws-java-sdk-core:1.11.469") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.access.key",configuration.get("AWS_ACCESS_KEY")) \
    .config("spark.hadoop.fs.s3a.serect.key",configuration.get("AWS_SERECT_KEY")) \
    .config("spark.hadoop.fs.s3a.aws.credentails.provider","org.apache.hadoop.fs.s3a.impl.SimpleAWSCredentailsProvider")\
    .getOrCreate()

    # adjust the log level to minimize the console output on executors 
    Spark.sparkContext.setLogLevel("WARN")
    
    # vechicle Sechma 
    vechicleSchema= StructType({
        StructField(name:"id", StringType(), nullable=True),
        StructField(name:"deviceId", StringType(), nullable=True),
        StructField(name:"timestamp", Timestamp(), nullable=True),
        StructField(name:"location", StringType(), nullable=True),
        StructField(name:"speed", DoubleType(), nullable=True),
        StructField(name:"direction", StringType(), nullable=True),
        StructField(name:"make", StringType(), nullable=True),
        StructField(name:"model", StringType(), nullable=True),
        StructField(name:"year", IntergerTyoe(), nullable=True),
        StructField(name:"fuelType", StringType(), nullable=True),
        
    })

    #gpsSchema

    vechicleSchema= StructType({
        StructField(name:"id", StringType(), nullable=True),
        StructField(name:"deviceId", StringType(), nullable=True),
        StructField(name:"timestamp", Timestamp(), nullable=True),
        StructField(name:"location", StringType(), nullable=True),
        StructField(name:"speed", DoubleType(), nullable=True),
        StructField(name:"direction", StringType(), nullable=True),
        StructField(name:"make", StringType(), nullable=True),
        StructField(name:"model", StringType(), nullable=True),
        StructField(name:"year", IntergerTyoe(), nullable=True),
        StructField(name:"fuelType", StringType(), nullable=True),
        
    })
    #trafficSchema
    trafficSchema= StructType({
        StructField(name:"id", StringType(), nullable=True),
        StructField(name:"deviceId", StringType(), nullable=True),
        StructField(name:"cameraId", StringTypetamp(), nullable=True),
        StructField(name:"location", StringType(), nullable=True),
        StructField(name:"timestamp", Timestamp(), nullable=True),
        StructField(name:"snapshot", StringType(), nullable=True),
    })
    #weatherSchema
    weatherSchema = StructType({
        StructField(name:"id", StringType(), nullable=True),
        StructField(name:"deviceId", StringType(), nullable=True),
        StructField(name:"timestamp", Timestamp(), nullable=True),
        StructField(name:"location", StringType(), nullable=True),
        StructField(name:"temperature", DoubleType(), nullable=True),
        StructField(name:"weatherCondition", StringType(), nullable=True),
        StructField(name:"precipitition", StringType(), nullable=True),
        StructField(name:"windSpeed", DoubleType(), nullable=True),
        StructField(name:"humidity", IntergerTyoe(), nullable=True),
        StructField(name:"airQuality", DoubleType(), nullable=True),
        
    })
    #emergencySchema
    emergencySchema = StringType({
        StructField(name:"id", StringType(), nullable=True),
        StructField(name:"deviceId", StringType(), nullable=True),
        StructField(name:"incidentId", StringType(), nullable=True),
        StructField(name:"type", StringType(), nullable=True),
        StructField(name:"timestamp", Timestamp(), nullable=True),
        StructField(name:"location", StringType(), nullable=True),
        StructField(name:"status", StringType(), nullable=True),
        StructField(name:"description", StringType(), nullable=True),

    })

    def read_kafka_topic(topic, sechma):
        return (Spark.readStream.format('kafka')
                .option(key:'kafka.bootstrap.servers'.Value:'broker:29092')
                .option('subscribe', topic).option(key:'startingOffsets', value:'earliest').load()
                .selectExpr('CAST(values AS STRING').select(from_json(col('value'),schema).alias('data'))
                .select('data.*')
                .withWatermark(eventTime:'timestamp', delayThreshold: '2 minutes')
        )

    def StreamWriter(input: DataFrame, checkpointFolder, output):
        return (input.writeStream
        .format("parquet")
        .option(key: 'checkpointlocation', 'checkpointFloder')
        .option(key: 'path', output)
        .outputMode('append')
        .start()
        )
    vehicleDF = read_kafka_topic(topic: 'vehicle_data',vechicleSchema).alias('vehicle')
    gpsDF = read_kafka_topic(topic: 'gps_data',vechicleSchema).alias('gps')
    trafficDF = read_kafka_topic(topic: 'traffic_data',vechicleSchema).alias('traffic')
    weatherDF = read_kafka_topic(topic: 'weather_data',vechicleSchema).alias('weather')
    emergencyDF = read_kafka_topic(topic: 'emergency_data',vechicleSchema).alias('emergency')

    query 1 = streamWriter(vechicle, checkpointFloder:'s3a://spark-streaming-data/chackpoint/vehicle_data', 
    output: 's3a://spark-streaming-data/data/vechicle_data')
    query 2 = streamWriter(gps_DF, checkpointFloder:'s3a://spark-streaming-data/chackpoint/gps_data', 
    output: 's3a://spark-streaming-data/data/gps_data')
    query 3 = streamWriter(traffic_DF, checkpointFloder:'s3a://spark-streaming-data/chackpoint/traffic_data', 
    output: 's3a://spark-streaming-data/data/traffic_data')
    query 4 = streamWriter(weather_DF, checkpointFloder:'s3a://spark-streaming-data/chackpoint/weather_data', 
    output: 's3a://spark-streaming-data/data/weather_data')
    query 5 = streamWriter(emergency_DF, checkpointFloder:'s3a://spark-streaming-data/chackpoint/emergency_data', 
    output: 's3a://spark-streaming-data/data/emergency_data')

if __name__== "__main__":
    main()
    