from nc.datahub.logging.data_hub_logging import DataHubLogger

# For comparison with Spark logs
# spark = SparkSession.builder.getOrCreate()


logger = DataHubLogger(__name__)


@logger.dh_log
def test_logging():
    logger.info('Function log')
    logger.warning('Function log')
    logger.debug('Function log')


if __name__ == '__main__':
    test_logging()
