from setupspark.utils.download_utils import (
    download_url, if_then_else, extract_data
)
import os
import sys
from logging import Logger


def download_and_install_hadoop(hadoop_version: str, hadoop_path: str, logger: Logger):
    """
    Function to download and install Hadoop
    :param hadoop_version: Version of Hadoop to Install
    :param hadoop_path: Path to Install Hadoop
    :param logger: The Logger Object to use for Logging
    :return: Returns Hadoop Home path if installed Successfully
    """

    try:
        hadoop_url = f"https://archive.apache.org/dist/hadoop/common/hadoop-{hadoop_version}/hadoop-{hadoop_version}.tar.gz"
        hadoop_dir = f"hadoop-{hadoop_version}"
        hadoop_home = os.environ.get('HADOOP_HOME', None)
        hadoop_home = if_then_else(hadoop_home is None, os.path.join(hadoop_path, hadoop_dir), hadoop_home)
        if os.path.exists(hadoop_path):
            pass
        else:
            os.makedirs(hadoop_path, 777)
        logger.info(f"Using {hadoop_home} as HADOOP_HOME")
        if os.path.exists(hadoop_home):
            logger.warning("The HADOOP Installation already exists. Skipping Installation")
        else:
            logger.warning(f"Downloading Hadoop from {hadoop_url}")
            logger.info(f"Downloading and Installing Hadoop to {hadoop_home}")
            hadoop_filename = os.path.join(hadoop_path, hadoop_url.split("/")[-1])
            if not os.path.exists(hadoop_filename):
                hadoop_filename = download_url(hadoop_url, hadoop_filename, logger)
            extract_data(hadoop_filename, hadoop_path)
        platform = sys.platform
        if 'win' in platform:
            download_winutils(hadoop_version, hadoop_home, logger)
        os.environ['HADOOP_HOME'] = hadoop_home
        os.environ["HADOOP_USER_CLASSPATH_FIRST"] = "true"
        os.environ["PATH"] = (
                os.environ.get("PATH", None) + ';' +
                os.path.join(hadoop_home, 'bin') + ';' +
                os.path.join(hadoop_home, 'sbin')
        )
        return hadoop_home
    except Exception as e:
        logger.error(e)
        raise e


def download_winutils(hadoop_version: str, hadoop_home: str, logger: Logger) -> bool:
    """
    Function to donwload winutils for Windows
    :param hadoop_version: Version of Hadoop to Install
    :param hadoop_home: Path to Hadoop Installation (HADOOP_HOME)
    :param logger: The Logger Object to use for Logging
    :return: Returns True if successful else False
    """

    try:
        winutils_url = f"https://github.com/cdarlint/winutils/raw/master/hadoop-{hadoop_version}/winutils.exe"
        winutils_path = os.path.join(hadoop_home, 'bin', winutils_url.split("/")[-1])
        if not os.path.exists(winutils_path):
            logger.warning("The winutils is not available in the given Path. Proceeding to download winutils")
            logger.info(f"Downloading and Installing Winutils from {winutils_url} to {os.path.join(hadoop_home, 'bin')}")
            download_url(winutils_url, winutils_path, logger)
        return True
    except Exception as e:
        logger.error(e)
        raise e
