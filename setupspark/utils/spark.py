from setupspark.utils.logger import get_logger
import tarfile
import os
import argparse
import wget
import subprocess
import sys
from urllib.request import Request, urlopen
import requests
# from fastprogress.fastprogress import progress_bar
from tqdm import tqdm


def download_and_install_spark(spark_version, hadoop_version, spark_path, logger):
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
        logger.warning(f"Downloading Spark from {spark_url}")
        spark_dir = f"spark-{spark_version}-bin-hadoop{spark_hadoop_version}"
        spark_home = os.environ.get('SPARK_HOME', None)
        spark_home = _if_then_else(spark_home, spark_home, os.path.join(spark_path, spark_dir))
        if os.path.exists(spark_path):
            pass
        else:
            os.makedirs(spark_path, 777)
        logger.info(f"Using {spark_home} as SPARK_HOME")
        if os.path.exists(spark_home):
            logger.warning("The given Path already exists. Skipping Installation")
        else:
            logger.info(f"Downloading and Installing Spark to {spark_home}")
            filename = _download_url(spark_url, os.path.join(spark_path, spark_url.split("/")[-1]), logger)
            spark_tf = tarfile.open(filename)
            spark_tf.extractall(path=spark_path)
            os.remove(filename)
        os.environ['SPARK_HOME'] = spark_home
        return spark_home
    except Exception as e:
        logger.error(e)
        raise e


def download_and_install_hadoop(hadoop_version, hadoop_path, logger):
    """
    Function to download and install Hadoop
    :param hadoop_version: Version of Hadoop to Install
    :param hadoop_path: Path to Install Hadoop
    :param logger: The Logger Object to use for Logging
    :return: Returns Hadoop Home path if installed Successfully
    """

    try:
        hadoop_url = f"https://archive.apache.org/dist/hadoop/common/hadoop-{hadoop_version}/hadoop-{hadoop_version}.tar.gz"
        logger.warning(f"Downloading Hadoop from {hadoop_url}")
        hadoop_dir = f"hadoop-{hadoop_version}"
        hadoop_home = os.environ.get('HADOOP_HOME', None)
        hadoop_home = _if_then_else(hadoop_home, hadoop_home, os.path.join(hadoop_path, hadoop_dir))
        if os.path.exists(hadoop_path):
            pass
        else:
            os.makedirs(hadoop_path, 777)
        logger.info(f"Using {hadoop_home} as HADOOP_HOME")
        if os.path.exists(hadoop_home):
            logger.warning("The given Path already exists. Skipping Installation")
        else:
            logger.info(f"Downloading and Installing Hadoop to {hadoop_home}")
            filename = _download_url(hadoop_url, os.path.join(hadoop_path, hadoop_url.split("/")[-1]), logger)
            hadoop_tf = tarfile.open(filename)
            hadoop_tf.extractall(path=hadoop_path)
            os.remove(filename)
        os.environ['HADOOP_HOME'] = hadoop_home
        return hadoop_home
    except Exception as e:
        logger.error(e)
        raise e


def _download_url(url, dest, logger, show_progress=False, chunk_size=1024 * 1024, timeout=5, retries=5):
    """
    Function to download the object from given url
    (Inspired from FastAI)
    :param url: The url to download the file from
    :param logger: The Logger Object to use for Logging
    :param chunk_size: chunk_size to read from url
    :param timeout: Timeout for URL read
    :param retries: Number of Retries for URL read
    :return: Returns the filename of the downloaded file
    """
    s = requests.Session()
    s.mount('http://', requests.adapters.HTTPAdapter(max_retries=retries))
    # additional line to identify as a firefox browser, see fastai/#2438
    s.headers.update({'User-Agent': 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:71.0) Gecko/20100101 Firefox/71.0'})
    u = s.get(url, stream=True, timeout=timeout)
    try:
        file_size = int(u.headers["Content-Length"])
    except Exception as e:
        show_progress = False
    show_progress = False
    try:
        logger.warning(f"Writing downloaded data to {dest}")
        with open(dest, 'wb') as f:
            nbytes = 0
            #        if show_progress:
            #           pbar = progress_bar(range(file_size), leave=False, parent=None)
            pbar = None
            if show_progress:
                pbar.update(0)
            for chunk in tqdm(u.iter_content(chunk_size=chunk_size)):
                nbytes += len(chunk)
                if show_progress:
                    pbar.update(nbytes)
                f.write(chunk)
        return dest
    except requests.exceptions.ConnectionError as e:
        filename, data_dir = os.path.split(dest)
        logger.warning(f"\n Download of {url} has failed after {retries} retries\n"
                       f" Fix the download manually:\n"
                       f"$ mkdir -p {data_dir}\n"
                       f"$ cd {data_dir}\n"
                       f"$ wget -c {url}\n"
                       f"$ tar xf {filename}\n"
                       f" And re-run your code once the download is successful\n")
        logger.error(e)
        raise e


def _if_then_else(condition, true_st, false_st):
    """
    Function to execute If Then Else Statements
    :param condition: Condition to execute
    :param true_st: Statement to execute when confition is true
    :param false_st: Statement to execute when condition is False
    :return:
    """

    return true_st if condition else false_st


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        prog='spark', description="Arguments Parser for Downloading and Installing Spark",
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
        '--hadoop-path', type=str, help="Path to Install Hadoop", default='resources\\hadoop', dest='hadoop_path'
    )
    parser.add_argument(
        '--spark-path', type=str, help="Path to Install Spark", default='resources\\spark', dest='spark_path'
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

    if args.java_home:
        os.environ['JAVA_HOME'] = args.java_home

    if os.environ.get('JAVA_HOME', None):
        logger.info("JAVA_HOME found in environment. Proceeding the setup process.")
        if args.hadoop:
            download_and_install_hadoop(args.hadoop_version, args.hadoop_path, logger)
        download_and_install_spark(args.spark_version, args.hadoop_version, args.spark_path, logger)
    else:
        logger.warning("JAVA_HOME is not found. Exiting the Process.")
