import logging


def get_logger(loglevel: str="INFO"):
    """
    Function to craete logger with given loglevel
    :param loglevel: The loglevel to use
    :return: Returns the Logger Object
    """

    logger = logging.getLogger('SetupSpark')
    if loglevel.upper() in ["INFO", "WARN", "ERROR", "DEBUG"]:
        logger.setLevel(loglevel)
    else:
        logger.setLevel("ERROR")
    s_handler = logging.StreamHandler()

    # Using format similar to Spark Log format
    formatter = logging.Formatter(
        '%(asctime)s %(levelname)s %(funcName)s: %(message)s',
        datefmt='%y/%m/%d %H:%M:%S'
    )
    s_handler.setFormatter(formatter)
    logger.addHandler(s_handler)

    return logger
