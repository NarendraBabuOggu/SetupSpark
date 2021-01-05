import argparse
from setupspark.utils.logger import get_logger
from logging import Logger
import os
from setupspark.spark import download_and_install_spark
from setupspark.hadoop import download_and_install_hadoop


def setup(
        spark_version: str = None, hadoop_version: str = None,
        spark_path: str = None, hadoop_path: str = None,
        install_hadoop: bool = False, loglevel: str = "WARN",
        java_home: str = None, logger: Logger = None
) -> bool:
    """
    Function to Setup Spark using the given arguments
    :param args: Arguments to use for setting up Spark
    :param logger: Logger Object to use for Logging
    :param spark_version: Version of Spark to Install
    :param hadoop_version: Version of Hadoop to install
    :param spark_path: Spark Installation Path
    :param hadoop_path: Hadoop Installation Path
    :param install_hadoop: Whether to Install Hadoop or Not
    :param loglevel: Loglevel to use for Logging
    :param java_home: Java Home to use
    :return: Completes the Setup and returns True if successful
    """

    try:
        logger.info("Starting the Setup process")
        if java_home:
            os.environ['JAVA_HOME'] = java_home

        if os.environ.get('JAVA_HOME', None):
            logger.info("JAVA_HOME found in environment. Proceeding the setup process.")
            if install_hadoop:
                logger.info("Setting Up Hadoop in {}".format(hadoop_path))
                download_and_install_hadoop(hadoop_version, hadoop_path, logger)
            logger.info("Setting Up Spark in {}".format(spark_path))
            download_and_install_spark(spark_version, hadoop_version, spark_path, logger)
        else:
            logger.warning("JAVA_HOME is not found. Exiting the Process.")
        return True
    except Exception as e:
        logger.error(e)
        raise e


def run():
    """
    Function to Run the Setup Process using given arguments
    :return: Returns True if successful else False.
    """

    parser = argparse.ArgumentParser(
        prog='setup.py', description="Arguments Parser for Setting Spark",
        epilog="Thanks for Using.", add_help=True, fromfile_prefix_chars='@'
    )

    parser.add_argument(
        '-s', '--spark-version', type=str, help="Spark Version to Install", required=False, dest='spark_version'
    )
    parser.add_argument(
        '--hadoop', action='store_true', default=False, help="Whether to Install Hadoop or Not", dest='hadoop'
    )
    parser.add_argument(
        '--hadoop-version', type=str, help="Hadoop Version to Install", required=False, dest='hadoop_version'
    )
    parser.add_argument(
        '--hadoop-path', type=str, help="Path to Install Hadoop", required=False, dest='hadoop_path'
    )
    parser.add_argument(
        '--spark-path', type=str, help="Path to Install Spark", required=False, dest='spark_path'
    )
    parser.add_argument(
        '-l', '--loglevel', type=str, help="Log Level to use for Logging", default='INFO',
        choices=["DEBUG", "INFO", "WARN", "ERROR"], dest='loglevel'
    )
    parser.add_argument(
        '--java-home', type=str, help="Path of Java Installation", required=False, dest='java_home'
    )
    parser.add_argument(
        '-c', '--config', type=str, help="Path of Configuration file containing the arguments",
        required=False, dest='config'
    )

    args = parser.parse_args()
    logger = get_logger(args.loglevel)

    if args.config:
        args = parser.parse_args(['@' + args.config])

    return setup(
        args.spark_version, args.hadoop_version, args.spark_path,
        args.hadoop_path, args.hadoop, args.loglevel, args.java_home,
        logger
    )


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='setup.py', description="Arguments Parser for Setting Spark",
        epilog="Thanks for Using.", add_help=True, fromfile_prefix_chars='@'
    )

    parser.add_argument(
        '-s', '--spark-version', type=str, help="Spark Version to Install", required=False, dest='spark_version'
    )
    parser.add_argument(
        '--hadoop', action='store_true', default=False, help="Whether to Install Hadoop or Not", dest='hadoop'
    )
    parser.add_argument(
        '--hadoop-version', type=str, help="Hadoop Version to Install", required=False, dest='hadoop_version'
    )
    parser.add_argument(
        '--hadoop-path', type=str, help="Path to Install Hadoop", required=False, dest='hadoop_path'
    )
    parser.add_argument(
        '--spark-path', type=str, help="Path to Install Spark", required=False, dest='spark_path'
    )
    parser.add_argument(
        '-l', '--loglevel', type=str, help="Log Level to use for Logging", default='INFO',
        choices=["DEBUG", "INFO", "WARN", "ERROR"], dest='loglevel'
    )
    parser.add_argument(
        '--java-home', type=str, help="Path of Java Installation", required=False, dest='java_home'
    )
    parser.add_argument(
        '-c', '--config', type=str, help="Path of Configuration file containing the arguments",
        required=False, dest='config'
    )

    args = parser.parse_args()
    logger = get_logger(args.loglevel)

    if args.config:
        args = parser.parse_args(['@' + args.config])

    setup(
        args.spark_version, args.hadoop_version, args.spark_path,
        args.hadoop_path, args.hadoop, args.loglevel, args.java_home,
        logger
    )
