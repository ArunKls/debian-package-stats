##################################################
"""
Utility for downloading and processing Debian package files.
Functions:
    download_files:
    Download files from the provided URLs asynchronously.

    decompress_file:
    Decompress a gzipped file and yield its content line by line.

    process_data:
    Process a line of data and extract file name and package names.

    download_and_process_files:
    Download and process multiple files asynchronously, updating package statistics.

    main:
    Main function to initiate the download and processing of Debian package files.

"""
##################################################


from collections import defaultdict
import os
import gzip
import time
import requests
from .common_utils import (
    get_contents_file_list, process_contents_file_list, filter_files, return_stats)

BUF_SIZE = 1024
SEC_IN_DAY = 86400
MIRROR = "http://ftp.uk.debian.org/debian/dists/stable/main/"


def download_files(urls, output_dir):
    """
    Download files from the provided URLs asynchronously.

    Args:
        urls (list): List of URLs to download.
        output_dir (str): Directory where the files will be stored.

    Returns:
        list: List of paths to the downloaded files.

    """
    # Iterate through each URL and download the file asynchronously
    # If skip_download is specified, check if the file exists and is recent before downloading
    output_paths = []
    try:
        for url in urls:
            file_name = os.path.basename(url)
            output_path = os.path.join(output_dir, file_name)
            # Stream for large files
            response = requests.get(url, stream=True, timeout=10)

            if response.status_code == 200:
                with open(output_path, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=BUF_SIZE):
                        f.write(chunk)

                output_paths.append(output_path)

            else:
                print(
                    f"Error fetching {url}: status code {response.status_code}")
                return None

    except requests.exceptions.RequestException as e:
        print(f"Error downloading {url}: {e}")
        return None
    return output_paths


def decompress_file(file_path):
    """
    Decompress a gzipped file and yield its content line by line.

    Args:
        file_path (str): Path to the gzipped file.

    Yields:
        str: Line of decompressed data.

    """
    # Open the gzipped file and yield its content line by line
    with gzip.open(file_path, 'rb') as f:
        for chunk in f.readlines():
            unzipped_data = chunk.decode()
            yield unzipped_data


def process_data(line):
    """
    Process a line of data and extract file name and package names.

    Args:
        line (str): Line of data.

    Returns:
        tuple: File name and list of package names.

    """
    # Extract file name and package names from the line

    line = line.strip()
    file_name, package_names = line.rsplit(maxsplit=1)
    package_names_list = package_names.split(",")
    if file_name != 'EMPTY_PACKAGE':
        return package_names_list
    return []


def download_and_process_files(files, arch, include_udeb, output_dir):
    """
    Download and process multiple files asynchronously.

    Args:
        files (list): List of file URLs to download and process.
        arch (str): Architecture of the packages to parse.
        include_udeb (bool): Flag to include udeb files for architecture.
        output_dir (str): Download location for content files.

    Returns:
        dict: Dictionary containing package statistics.

    """
    # Get the list of files and filter based on architecture and include_udeb flag
    # Download files asynchronously and process them, updating package_stats_dict
    package_stats_dict = defaultdict(int)
    urls = filter_files(files, arch, include_udeb)
    # print("urls", urls)
    download_paths = download_files(urls, output_dir)

    for download_path in download_paths:
        for data in decompress_file(download_path):
            packages_list = process_data(data)
            for package in packages_list:
                # package_stats_dict[package].append(file_name)
                package_stats_dict[package] += 1
    return package_stats_dict


def main():
    """
    Main function to initiate the download and processing of Debian package files.

    """
    # Get the list of files, download, and process them
    # Print the package statistics and execution time
    start = time.time()
    files = get_contents_file_list(MIRROR)
    files = process_contents_file_list(MIRROR, files)
    # print("files", files)
    stats_dict = download_and_process_files(
        files, "arm64", True, "./downloads")
    stats = return_stats(stats_dict, True, 20)
    print(stats)
    print(time.time() - start)


if __name__ == "__main__":
    main()
