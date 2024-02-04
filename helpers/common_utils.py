###########################################################
"""
Utility functions for working with Debian package files.
Functions:
    get_contents_file_list:
        Get a list of file links from the given URL using BeautifulSoup.

    process_contents_file_list:
        Process the list of file links and organize them by architecture.

    filter_files:
        Filter files based on architecture and include_udeb flag.

    return_stats:
        Return formatted package statistics.

    write_a_file_with_unit_tests:
        Placeholder for writing unit tests (not implemented in the provided code).
"""
###########################################################

from collections import defaultdict
import os
import requests
from bs4 import BeautifulSoup

def get_contents_file_list(url):
    """
    Get a list of file links from the given URL using BeautifulSoup.

    Args:
        url (str): The URL to scrape for file links.

    Returns:
        list: List of file links.

    """
    try:
        response = requests.get(url, timeout=10)
        response.raise_for_status()  # Check for errors in the HTTP response

        soup = BeautifulSoup(response.content, 'html.parser')

        file_links = soup.find_all('a', href=True) # Parse to find links

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
    return file_links


def process_contents_file_list(url, file_links):
    """
    Process the list of file links and organize them by architecture.

    Args:
        url (str): The base URL for the files.
        file_links (list): List of file links.

    Returns:
        dict: Dictionary containing files organized by architecture.

    """
    files = defaultdict(list)
    for link in file_links:
        file_url = link['href']
        if file_url.endswith(".gz"):
            file_name = link.text
            file_name_wo_ext, ext = os.path.splitext(file_name)
            file_name_split = file_name_wo_ext.split("-")
            arch = file_name_split[-1]
            # construct dictionary indexed by architecture
            files[arch].append(
                {
                    "name": link['href'],
                    "link": os.path.join(url, link.text),
                    "udeb": "udeb" in file_name_split
                }
            )
    return files


def filter_files(files, arch, include_udeb, all_files=False):
    """
    Filter files based on architecture and include_udeb flag.

    Args:
        files (dict): Dictionary containing files organized by architecture.
        arch (str): Architecture to filter files.
        include_udeb (bool): Flag to include udeb files.
        all_files (bool): Ignore filters and return urls of all files - For testing
    Returns:
        tuple: Tuple containing lists of URLs and names for filtered files.

    """
    urls = []
    if not all_files:
        for file in files[arch]:
            if include_udeb or (not file.get("udeb")):
                urls.append(file.get("link"))
        return urls
    for arch in files.keys():
        for file in files[arch]:
            urls.append(file.get("link"))
    return urls


def return_stats(package_stats, descending, count):
    """
    Return formatted package statistics.

    Args:
        package_stats (dict): Dictionary containing package statistics.
        descending (bool): Flag to indicate sorting order.
        count (int): Number of top packages to display.

    Returns:
        str: Formatted package statistics.

    """
    sorted_stats = sorted(package_stats.items(),
                          key=lambda x: x[1], reverse=descending)
    output = [f"{'Package':50} \t File Count"]
    for line in range(min(count, len(sorted_stats))):
        output.append(f"{sorted_stats[line][0]:50} \t {sorted_stats[line][1]}")
    return "\n".join(output)
