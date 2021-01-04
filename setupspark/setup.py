import requests
import argparse
from setupspark.utils.logger import get_logger
import os
from setupspark.utils.spark import download_and_install_spark, download_and_install_hadoop


def setup(args, logger):
    """
    Function to Setup Spark using the given arguments
    :param args: Arguments to use for setting up Spark
    :param logger: Logger Object to use for Logging
    :return: Completes the Setup and returns True
    """
    try:
        logger.info("Starting the Setup process")
        if args.java_home:
            os.environ['JAVA_HOME'] = args.java_home

        if os.environ.get('JAVA_HOME', None):
            logger.info("JAVA_HOME found in environment. Proceeding the setup process.")
            if args.hadoop:
                logger.info("Setting Up Hadoop in {}".format(args.hadoop_path))
                download_and_install_hadoop(args.hadoop_version, args.hadoop_path, logger)
            logger.info("Setting Up Spark in {}".format(args.spark_path))
            download_and_install_spark(args.spark_version, args.hadoop_version, args.spark_path, logger)
        else:
            logger.warning("JAVA_HOME is not found. Exiting the Process.")
        return True
    except Exception as e:
        logger.error(e)
        raise e


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='setup.py', description="Arguments Parser for Setting Spark",
        epilog="Thanks for Using.", add_help=True
    )

    parser.add_argument(
        '-s', '--spark-version', type=str, help="Spark Version to Install", default='2.4.7', dest='spark_version'
    )
    parser.add_argument(
        '--hadoop', action='store_true', default=False, help="Whether to Install Hadoop or Not", dest='hadoop'
    )
    parser.add_argument(
        '--hadoop-version', type=str, help="Hadoop Version to Install", default='2.7.0', dest='hadoop_version'
    )
    parser.add_argument(
        '--hadoop-path', type=str, help="Path to Install Hadoop", default='..\\resources\\hadoop', dest='hadoop_path'
    )
    parser.add_argument(
        '--spark-path', type=str, help="Path to Install Spark", default='..\\resources\\spark', dest='spark_path'
    )
    parser.add_argument(
        '-l', '--loglevel', type=str, help="Log Level to use for Logging", default='INFO', dest='loglevel'
    )
    parser.add_argument(
        '--java-home', type=str, help="Path of Java Installation", default="C:\\Program Files\\OpenJDK12",
        dest='java_home'
    )

    args = parser.parse_args()
    logger = get_logger(args.loglevel)

    setup(args, logger)
