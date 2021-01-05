import os
import requests
import tarfile
from typing import Callable, Optional, Any
from logging import Logger
try:
    from fastprogress.fastprogress import progress_bar
    SHOW_PROGRESS = True
except ImportError:
    SHOW_PROGRESS = False
    print("FastProgress is not found. Please try to install it Manually")


def download_url(
        url: str, dest: str, logger: Logger,
        show_progress: bool = SHOW_PROGRESS,
        chunk_size: int= 1024 * 1024,
        timeout: int= 5, pbar: Callable = None,
        retries=5
) -> Optional[str]:
    """
    Function to download the object from given url
    (Inspired from FastAI)

    :param url: The url to download the file from
    :param logger: The Logger Object to use for Logging
    :param chunk_size: chunk_size to read from url
    :param timeout: Timeout for URL read
    :param retries: Number of Retries for URL read
    :param pbar: Parent Progress Bar to use
    :param dest: The destination to download the url
    :param show_progress: Whether to show the progress of downloading url or not
    :return: Returns the destination of the downloaded file if successful
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
    try:
        logger.warning(f"Writing downloaded data to {dest}")
        with open(dest, 'wb') as f:
            nbytes = 0
            if show_progress:
                pbar = progress_bar(range(file_size), leave=False, parent=pbar)
                pbar.update(0)
            for chunk in u.iter_content(chunk_size=chunk_size):
                nbytes += len(chunk)
                if show_progress:
                    pbar.update(nbytes)
                f.write(chunk)
            if show_progress:
                pbar.comment("\n")
        logger.info("Download is Complete.")
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


def if_then_else(condition: bool, true_st: Any, false_st: Any) -> Any:
    """
    Function to execute If Then Else Statements
    :param condition: Condition to execute
    :param true_st: Statement to execute when confition is true
    :param false_st: Statement to execute when condition is False
    :return: Returns true_st if condition is True and false_st if condition is False
    """

    return true_st if condition else false_st


def extract_data(filename: str, path: str, logger: Logger) -> bool:
    """
    Function to Extract data from given file to given path
    :param filename: Filename containing the compressed data
    :param path: Path to extract the data
    :param logger: The Logger Object to use for Logging
    :return: Returns True if Success and None if failed.
    """

    try:
        tf = tarfile.open(filename)
        tf.extractall(path=path)
        try:
            os.remove(filename)
        except PermissionError as e:
            logger.warning(f"Unable to delete the downloaded file {filename}. Please delete it manually")
            logger.error(e)
        return True
    except Exception as e:
        raise e
