from setupspark.utils.download_utils import (
    download_url, if_then_else, extract_data
)
import os
import sys
from logging import Logger


def download_and_install_spark(spark_version: str, hadoop_version: str, spark_path: str, logger: Logger) -> str:
    """
    Function to download and install Spark
    :param spark_version: Version of Spark to Install
    :param hadoop_version: Version of Hadoop to use for Spark
    :param spark_path: Path to install spark
    :param logger: The Logger Object to use for Logging
    :return: Returns Spark Home path if installed Successfully
    """

    try:
        spark_hadoop_version = hadoop_version[:-2]
        spark_url = f"https://downloads.apache.org/spark//spark-{spark_version}/spark-{spark_version}-bin-hadoop{spark_hadoop_version}.tgz"
        spark_dir = f"spark-{spark_version}-bin-hadoop{spark_hadoop_version}"
        spark_home = os.environ.get('SPARK_HOME', None)
        spark_home = if_then_else(spark_home is None, os.path.join(spark_path, spark_dir), spark_home)
        if os.path.exists(spark_path):
            pass
        else:
            os.makedirs(spark_path, 777)
        logger.info(f"Using {spark_home} as SPARK_HOME")
        if os.path.exists(spark_home):
            logger.warning("The Spark Installation already exists. Skipping Installation")
        else:
            logger.warning(f"Downloading Spark from {spark_url}")
            logger.info(f"Downloading and Installing Spark to {spark_home}")
            spark_filename = os.path.join(spark_path, spark_url.split("/")[-1])
            if not os.path.exists(spark_filename):
                spark_filename = download_url(spark_url, spark_filename, logger)
            extract_data(spark_filename, spark_path)
        os.environ['SPARK_HOME'] = spark_home
        os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
        os.environ["PATH"] = (
                os.environ.get("PATH", None) + ';' +
                os.path.join(spark_home, 'bin') + ';' +
                os.path.join(spark_home, 'sbin')
        )
        return spark_home
    except Exception as e:
        logger.error(e)
        raise e
