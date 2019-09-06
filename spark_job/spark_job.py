from pyspark.sql import SparkSession
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql.types import IntegerType
import json
import logging
import os


def setup_custom_logger(filename):
    """Set configuration for logging"""

    logger = logging.getLogger('root')
    logger.setLevel(logging.INFO)

    # set file output handler and formatter for that
    file_handler = logging.FileHandler(filename)
    file_handler.setFormatter(logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s'))

    # set console output handler and formatter
    console_handler = logging.StreamHandler()
    console_handler.setFormatter(logging.Formatter('%(asctime)s -  %(message)s'))

    logger.addHandler(console_handler)
    logger.addHandler(file_handler)

    return logger


if __name__ == "__main__":

    KAFKA_BROKERS = os.environ.get('KAFKA_BROKERS')

    KAFKA_TOPIC = os.environ.get('KAFKA_TOPIC')

    SPARK_STREAMING_DELAY = os.environ.get('SPARK_STREAMING_DELAY')

    MONGODB_URI = os.environ.get('MONGODB_URI')
    MONGODB_DATABASE = os.environ.get('MONGODB_DATABASE')
    MONGODB_COLLECTION = os.environ.get('MONGODB_COLLECTION')

    SPARK_LOGS = os.environ.get('SPARK_LOGS')

    # init logger
    LOGGER = setup_custom_logger(SPARK_LOGS)

    LOGGER.info('Starting Spark session...')

    # create spark session
    spark = SparkSession \
        .builder \
        .appName("BigDataAnalyzer") \
        .config("spark.mongodb.output.uri", MONGODB_URI) \
        .config("spark.mongodb.output.database", MONGODB_DATABASE) \
        .config("spark.mongodb.output.collection", MONGODB_COLLECTION) \
        .config("ark.mongodb.output.partitionerOptions.partitionKey", "id") \
        .getOrCreate()

    LOGGER.info('Spark session started')

    # init spark context
    sc = spark.sparkContext

    # init streaming spark context
    streaming_sc = StreamingContext(sc, int(SPARK_STREAMING_DELAY))

    LOGGER.info('Creating direct stream to kafka cluster......')

    # create direct stream to kafka cluster
    kafka_stream = KafkaUtils.createDirectStream(streaming_sc, [KAFKA_TOPIC],
                                                 {"metadata.broker.list": (
                                                     ','.join(str(x) for x in KAFKA_BROKERS.split()))})

    LOGGER.info('Direct stream to kafka cluster created')

    # extract messages
    messages = kafka_stream.map(lambda x: x[1])

    LOGGER.info('{} messages received'.format(messages.count()))

    def func(rdd):
        """Handle spark rdd data and save it to database"""
        if not rdd.isEmpty():
            df = spark.read.json(sc.parallelize([json.loads(row) for row in rdd.collect()]))
            # df = df.select(col)'id'].cast(IntegerType())
            df = df.withColumn('id', df.id.cast(IntegerType()))

            try:
                LOGGER.info('Saving messages to database...')
                df.write.format("mongo").mode("append").save()
                LOGGER.INFO("Saving messages to database completed successfully")
            except Exception as err:
                LOGGER.error('Error saving messages to database')


    if messages.count() is not 0:
        LOGGER.info('Handling messages')
        messages.foreachRDD(func)

    # start listening
    streaming_sc.start()
    streaming_sc.awaitTermination()
