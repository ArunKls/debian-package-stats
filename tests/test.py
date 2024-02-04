#####################################################
"""
Module for unit tests for package stats helpers and common utility functions
"""
#####################################################


import unittest
from unittest.mock import patch
import requests
from helpers.common_utils import (
    get_contents_file_list, process_contents_file_list, filter_files, return_stats
)

class TestCommonUtils(unittest.TestCase):
    """
    Class for Package statistics unit tests
    """
    # Test get_contents_file_list with successful HTML parsing
    @patch('requests.get')
    def test_get_contents_file_list_success(self, mock_get):
        """
        Method to test valid case for get_contents_file_list
        """
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b'<html><a href="file1.gz">file1.gz</a></html>'
        mock_get.return_value = mock_response

        file_links = get_contents_file_list("http://example.com")
        self.assertEqual(file_links, ['file1.gz'])

    # Test get_contents_file_list with error handling
    @patch('requests.get')
    def test_get_contents_file_list_error(self, mock_get):
        """
        Method to test invalid case for get_contents_file_list
        """
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with self.assertRaises(requests.exceptions.RequestException):
            get_contents_file_list("http://example.com")

    # Test process_contents_file_list with valid data
    def test_process_contents_file_list_valid(self):
        """
        Method to test valid case for process_contents_file_list
        """
        files = ['Contents-udeb-arch1.gz', 'Contents-udeb-arch2.gz',
                 'Contents-arch1.gz', 'Contents-arch1.gz']
        expected_files = {
            'arch1': [
                {'name': 'Contents-udeb-arch1.gz',
                 'link': 'https://example.com/Contents-udeb-arch1.gz', 
                 'udeb': True},
                {'name': 'Contents-arch1.gz',
                 'link': 'https://example.com/Contents-arch1.gz', 
                 'udeb': False}],
            'arch2': [
                {'name': 'Contents-udeb-arch2.gz',
                 'link': 'https://example.com/Contents-udeb-arch2.gz', 
                 'udeb': True},
                {'name': 'Contents-arch2.gz',
                 'link': 'https://example.com/Contents-arch2.gz', 
                 'udeb': False}]
        }

        processed_files = process_contents_file_list("https://example.com", files)
        self.assertEqual(processed_files, expected_files)

    def test_filter_files_udeb(self):
        """
        Method to test filter files
        """
        files = {
            'arch1': [
                {'name': 'Contents-udeb-arch1.gz',
                 'link': 'https://example.com/Contents-udeb-arch1.gz', 
                 'udeb': True},
                {'name': 'Contents-arch1.gz',
                 'link': 'https://example.com/Contents-arch1.gz', 
                 'udeb': False}],
            'arch2': [
                {'name': 'Contents-udeb-arch2.gz',
                 'link': 'https://example.com/Contents-udeb-arch2.gz', 
                 'udeb': True},
                {'name': 'Contents-arch2.gz',
                 'link': 'https://example.com/Contents-arch2.gz', 
                 'udeb': False}]
        }
        expected_urls = ['https://example.com/Contents-udeb-arch2.gz',
                         'https://example.com/Contents-udeb-arch2.gz']
        urls = filter_files(files, 'arch2', include_udeb=True)
        self.assertEqual(urls, expected_urls)
        expected_urls = ['https://example.com/Contents-udeb-arch2.gz']
        urls = filter_files(files, 'arch2', include_udeb=False)
        self.assertEqual(urls, expected_urls)

    def test_return_stats(self):
        """
        Method to test return_stats function
        """
        stats = {'package1': 1, 'package2': 2, 'package3': 3, 'package4': 4}
        expected_output = [f"{'Package':50} \t File Count"]
        expected_output.append(f"{'package4':50} \t 4")
        expected_output.append(f"{'package3':50} \t 3")
        output = return_stats(stats, descending=True, count=2)
        self.assertEqual(output, expected_output)
