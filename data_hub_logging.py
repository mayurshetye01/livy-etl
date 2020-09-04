import inspect
import logging.config

DEFAULT_CONFIG = {
    'version': 1,
    'disable_existing_loggers': False,
    'formatters': {
        'verbose': {
            'format': '%(asctime)s %(levelname)s %(module)s: %(message)s',
            'datefmt': '%y/%m/%d %H:%M:%S'
        },
    },
    'handlers': {
        'console': {
            'level': 'INFO',
            'class': 'logging.StreamHandler',
            'formatter': 'verbose'
        },
    },
    'loggers': {
        '': {  # root logger
            'handlers': ['console'],
            'level': 'INFO',
            'propagate': False
        },
    }
}


class DataHubLogger(logging.getLoggerClass()):

    def __init__(self, name, conf=DEFAULT_CONFIG):
        logging.config.dictConfig(conf)
        self.logger = logging.getLogger(name)

    def __make_record(self, func, type: str):
        mod = inspect.getmodule(func)
        self.logger.info("%s <%s.%s.%s>", type, mod.__package__, mod.__name__, func.__name__)

    def __entry(self, func):
        """ Pre function logging """
        self.__make_record(func, "ENTRY")

    def __exit(self, func):
        """ Post function logging """
        self.__make_record(func, "EXIT")

    def dh_log(self, func):
        """ Wrapper function to log entry and exit for function/method call """

        def call(*args, **kwargs):
            """ Actual wrapping """
            self.__entry(func)
            result = func(*args, **kwargs)
            self.__exit(func)
            return result

        return call

    def info(self, msg, extra=None):
        self.logger.info(msg, extra=extra)

    def warning(self, msg, extra=None):
        self.logger.warning(msg, extra=extra)

    def debug(self, msg, extra=None):
        self.logger.debug(msg, extra=extra)

    def error(self, msg, extra=None):
        self.logger.error(msg, extra=extra)

    def critical(self, msg, extra=None):
        self.logger.critical(msg, extra=extra)

    def exception(self, msg, extra=None):
        self.logger.exception(msg, extra=extra)

    def fatal(self, msg, extra=None):
        self.logger.fatal(msg, extra=extra)
