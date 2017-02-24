import os
import logging
import ConfigParser
from logging.handlers import TimedRotatingFileHandler
from django.conf import settings


def get_job_time_out():
    config = ConfigParser.ConfigParser()
    config.read(settings.CONF_DIR + os.path.sep + 'system_config.ini')
    time_out = config.get('HADOOP_JOB', 'job_time_out')
    return int(time_out)


def get_logger(module_name):
    logger = logging.getLogger(module_name)
    config = ConfigParser.ConfigParser()
    config.read(settings.CONF_DIR + os.path.sep + 'system_config.ini')

    level = config.get('LOGGING', 'level')
    logger.setLevel(eval('logging.' + level))

    log_dir = config.get('LOGGING', 'path')

    # Window Testing - Comment Later.
    log_dir = os.getcwd()

    if not os.path.isdir(log_dir):
        os.mkdir(log_dir)

    logfile = log_dir + os.path.sep + 'di.log'

    formatter_str = config.get('LOGGING', 'formatter')
    formatter_str = formatter_str.replace('*', '%')

    # Set Logging Handler
    if not len(logger.handlers):
        # handler = logging.FileHandler(logfile)
        handler = TimedRotatingFileHandler(logfile, 'midnight', 1, backupCount=5)
        handler.setLevel(eval('logging.' + level))
        formatter = logging.Formatter(formatter_str)
        handler.setFormatter(formatter)

        # add the handlers to the logger
        logger.addHandler(handler)

    return logger