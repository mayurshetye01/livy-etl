from pyspark.sql import SparkSession
import logging


class LogETL:
   

    spark = SparkSession.builder.getOrCreate()
    sc = spark.sparkContext
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    log.warn("From log4j warn")
    log.info('From log4j info')
    
    logger = logging.getLogger('TestLogger')
    logger.info('From logging info')
    logger.warning('From logging warn')
