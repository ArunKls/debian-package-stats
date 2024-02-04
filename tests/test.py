import unittest
from unittest.mock import patch
from helpers.common_utils import *

class TestCommonUtils(unittest.TestCase):

    # Test get_contents_file_list with successful HTML parsing
    @patch('requests.get')
    def test_get_contents_file_list_success(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 200
        mock_response.content = b'<html><a href="file1.gz">file1.gz</a></html>'
        mock_get.return_value = mock_response

        file_links = get_contents_file_list("http://example.com")
        self.assertEqual(file_links, ['file1.gz'])

    # Test get_contents_file_list with error handling
    @patch('requests.get')
    def test_get_contents_file_list_error(self, mock_get):
        mock_response = unittest.mock.Mock()
        mock_response.status_code = 404
        mock_get.return_value = mock_response

        with self.assertRaises(requests.exceptions.RequestException):
            get_contents_file_list("http://example.com")

    # Test process_contents_file_list with valid data
    def test_process_contents_file_list_valid(self):
        files = {
            'amd64': [{'name': 'file1.gz', 'link': 'https://example.com/file1.gz', 'udeb': False}],
            'i386': [{'name': 'file2.gz', 'link': 'https://example.com/file2.gz', 'udeb': True}]
        }
        expected_files = {
            'amd64': [{'name': 'file1.gz', 'link': 'https://example.com/file1.gz', 'udeb': False}],
            'i386': [{'name': 'file2.gz', 'link': 'https://example.com/file2.gz', 'udeb': True}]
        }

        processed_files = process_contents_file_list("https://example.com", files)
        self.assertEqual(processed_files, expected_files)

    # Test process_contents_file_list with missing arch
    def test_process_contents_file_list_missing_arch(self):
        files = {'amd64': [{'name': 'file1.gz', 'link': 'https://example.com/file1.gz', 'udeb': False}]}
        with self.assertRaises(KeyError):
            process_contents_file_list("https://example.com", files)

    # Test process_contents_file_list with invalid file format
    def test_process_contents_file_list_invalid_format(self):
        files = {'amd64': [{'name': 'file.txt', 'link': 'https://example.com/file.txt', 'udeb': False}]}
        with self.assertRaises(ValueError):
            process_contents_file_list("https://example.com", files)
