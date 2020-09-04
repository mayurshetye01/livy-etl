from nc.datahub.logging.data_hub_logging import DataHubLogging

# For comparison with Spark logs
# spark = SparkSession.builder.getOrCreate()


logging = DataHubLogging()
logger = logging.get_logger(__name__)


@logging.dh_log
def test_logging():
    logger.info('Function log')
    logger.warning('Function log')
    logger.debug('Function log')


if __name__ == '__main__':
    test_logging()
