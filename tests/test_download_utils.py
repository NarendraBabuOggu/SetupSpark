from setupspark.utils.download_utils import (
    if_then_else, download_url
)
from setupspark.utils.logger import get_logger
import unittest
import os


class TestIfThenElse(unittest.TestCase):
    """
    Test Case to Validate if_then_else function
    """
    def test_true_statement(self):
        """
        Test Case method to check true statement functionality of if_then_else function
        """

        self.assertEqual(if_then_else(True, 1, 0), 1)
        self.assertTrue(if_then_else(True, True, False))

    def test_false_statement(self):
        """
        Test Case method to check false statement functionality of if_then_else function
        """

        self.assertEqual(if_then_else(False, 1, 0), 0)
        self.assertFalse(if_then_else(False, True, False))


class TestDownLoadUrl(unittest.TestCase):
    """
    Test Case to Validate download_url function
    """
    def test_with_out_progress(self):
        """
        Test Case method to check the functionality of download_url function when show_progress = False
        """
        logger = get_logger("WARN")
        self.assertIsNotNone(download_url(
            "https://raw.githubusercontent.com/NarendraBabuOggu/SetupSpark/main/README.md",
            "sample.txt",  logger=logger,
            show_progress=False,
        ))
        os.remove("sample.txt")

    def test_valid_url(self):
        """
        Test Case method to check the functionality of download_url function when valid URL is passed
        """

        logger = get_logger("WARN")
        self.assertEqual(download_url(
            "https://raw.githubusercontent.com/NarendraBabuOggu/SetupSpark/main/README.md",
            "sample.txt",  logger=logger,
            show_progress=False
        ), "sample.txt"
        )
        os.remove("sample.txt")

    def test_invalid_url(self):
        """
        Test Case method to check the functionality of download_url function when valid URL is passed
        """

        with self.assertRaises(Exception):
            logger = get_logger("WARN")
            self.assertIsNone(download_url(
                "https://githubusercontent.com/NarendraBabuOggu/SetupSpark/main/README.md",
                "sample.txt",  logger=logger,
                show_progress=False
            ))
