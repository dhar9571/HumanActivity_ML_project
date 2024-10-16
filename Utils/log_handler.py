import logging
import logging.handlers
from datetime import datetime
from Utils.params_utils import ParamsUtils
import os


class Logger:
    __config = ParamsUtils.get_configuration(file_name='GeneralConfig.yaml', section='log_setting')

    @staticmethod
    def configure_logger(logger_name, log_file_prefix, frmt, log_to_console=True, log_level=None):
        logger = logging.getLogger(logger_name)
        if not logger.hasHandlers():  # Prevent multiple handlers being added
            formatter = logging.Formatter(frmt, datefmt='%Y-%m-%d %H:%M:%S')

            # Get current date (for uniqueness of log file per date)
            current_date = datetime.now().strftime("%Y-%m-%d")

            # Extract the directory path from log_file_prefix
            log_directory = os.path.dirname(log_file_prefix)

            # Check if directory exists, if not, create it
            if not os.path.exists(log_directory):
                os.makedirs(log_directory)  # Create the directory and any parent directories if necessary

            # Combine date and time for the log file name, but keep the date in the log path
            log_file = f"{log_file_prefix}{current_date}.log"

            # TimedRotatingFileHandler for creating a new log file each day
            fileHandler = logging.handlers.TimedRotatingFileHandler(log_file, when='midnight', interval=1)
            fileHandler.suffix = "%Y-%m-%d"  # Log file name format based on date after rotation
            fileHandler.setFormatter(formatter)
            logger.addHandler(fileHandler)

            # Stream handler for logging to console (optional)
            if log_to_console:
                streamHandler = logging.StreamHandler()
                streamHandler.setFormatter(formatter)
                logger.addHandler(streamHandler)

            # Set log level
            if log_level is None:
                loglevel = Logger.get_log_level(Logger.__config['loglevel'])
            else:
                loglevel = Logger.get_log_level(log_level)

            logger.setLevel(loglevel)

        return logger
    


    @staticmethod
    def setup_logger(log_level=None):
       
        # Logger configuration (writes to daily log files)
        logger = Logger.configure_logger(
            'LOGGER', Logger.__config['logpath'],
            '%(asctime)s [%(filename)s:%(lineno)d]: %(message)s...',
            log_to_console=Logger.__config['log_to_console'],
            log_level=log_level
        )
        return logger
    

    # Function to map loglevel from YAML to Python logging levels
    @staticmethod
    def get_log_level(loglevel):
        
        log_level_mapping = {
            1: logging.DEBUG, 
            2: logging.ERROR
            }
        return log_level_mapping.get(loglevel, logging.DEBUG)


# logger = Logger.setup_logger() 

# logger.debug("processing started...")

# logger.error(f"Exception caught: {e}", exc_info=True)
        