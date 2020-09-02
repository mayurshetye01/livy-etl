from nc.datahub.logger import DataHubLogger
from nc.datahub.nc_logging import NCDataHubLogging

from pyspark.sql import SparkSession

# For testing
spark = SparkSession.builder.getOrCreate()

def test_with_logging():
    logger = NCDataHubLogging.get_logger(__name__)
    logger.info('Global NC logging Info')
    logger.warning('Global NC logging warn')
    logger.debug('Global NC logging debug')


def test_with_logger():
    logger = DataHubLogger(name=__name__)

    logger.info('Custom Logger Info')
    logger.warn('Custom Logger warn')
    logger.debug('Custom Logger debug')


if __name__ == '__main__':
    test_with_logging()
