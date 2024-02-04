############################################################
"""
Asynchronous utility for downloading and processing Debian package files.
Functions:

    download_file:
        Download a file asynchronously using aiohttp and aiofiles.

    process_file:
        Process a gzipped file asynchronously.

    mapper:
        Map function to process lines from the gzipped file asynchronously.

    download_and_process_files:
        Download and process multiple files asynchronously.

    main:
        Main asynchronous function to download and process Debian package files.

    package_stats:
        Calculate and print package statistics based on given parameters.

    cli:
        Command-line interface function to get package statistics.
"""
############################################################

from collections import defaultdict
import os
import asyncio
import gzip
import time
import argparse
from concurrent.futures import ThreadPoolExecutor
import aiohttp
import aiofiles
from .exceptions import DownloadError
from .common_utils import (
    get_contents_file_list, process_contents_file_list, filter_files, return_stats)

PARTITION = 5000
SEC_IN_DAY = 86400
MIRROR = "http://ftp.uk.debian.org/debian/dists/stable/main/"
package_stats_dict = defaultdict(int)


async def download_file(url, output_dir, skip_download):
    """
    Download a file asynchronously using aiohttp and aiofiles.

    Args:
        url (str): The URL of the file to download.
        output_dir (str): The directory where the downloaded file will be stored.
        skip_download (int): Number of days to skip download if the file exists and is recent.

    Returns:
        str: The path to the downloaded file or None if the download fails.

    """
    # Download file. skip if newer than skip_download days

    # print("Downloading file", url)
    file_name = os.path.basename(url)
    output_path = os.path.join(output_dir, file_name)
    if skip_download:
        if os.path.exists(output_path):
            # If file exists in path and is recently created, skip file download
            time_since_download = time.time() - os.path.getmtime(output_path)
            if time_since_download < skip_download * SEC_IN_DAY:
                # print("Found file. Skipping download")
                return output_path
    try:
        async with aiohttp.ClientSession() as session:
            async with session.get(url) as response:
                if response.status == 200:
                    filename = os.path.basename(url)
                    output_path = os.path.join(output_dir, filename)
                    # Asynchronously write to local for download
                    async with aiofiles.open(output_path, 'wb') as f:
                        await f.write(await response.read())
                    # print("Downloaded file", output_path)
                    return output_path

                else:
                    raise DownloadError(url, response.status)
    except Exception as e:
        raise DownloadError(url, e) from e


def process_file(file_path):
    """
    Process a gzipped file asynchronously in PARTITION sized 
    buffers and sends to mapper function.

    Args:
        file_path (str): The path to the gzipped file to process.

    """
    # Read and decompress file, send data in chunks to mapper

    # print("Processing file", file_path)
    with gzip.open(file_path, 'rb') as f:
        buffer = []
        # Decompress and read the file in buffers of size PARTITION
        for line in f.readlines():
            buffer.append(line.decode())
            if len(buffer) < PARTITION:
                continue
            else:
                # Send the buffer to mapper function
                mapper(buffer)
                buffer = []
        # Send the left over lines to mapper function
        mapper(buffer)

    # print("Processed file", file_path)


def mapper(lines):
    """
    Map function to process lines from the gzipped file asynchronously.
    Counts the occurences of packages and updates dict.
    Args:
        lines (list): List of lines from the gzipped file.

    """
    # Process each line to split, and count package occurences

    # print("mapper", len(lines))
    for line in lines:
        line = line.strip()
        file_name, package_names = line.rsplit(maxsplit=1)
        package_names_list = package_names.split(",")
        if file_name == 'EMPTY_PACKAGE':
            return
        # Updating package dictionary counts
        for package in package_names_list:
            package_stats_dict[package] += 1
    # print("mapper done")


async def download_and_process_files(urls, output_dir, skip_download):
    """
    Download and process multiple files asynchronously.

    Args:
        urls (list): File URLs to download and process.
        output_dir (str): Download location for content files.
        skip_download (int): Skip download if files are already present and newer than 's' days.

    """
    # Filter files according to architecture
    tasks = []
    for url in urls:
        # Add download and process tasks to list
        tasks.append(asyncio.create_task(
            download_file(url, output_dir, skip_download)))
    # wait download and process tasks
    for task in asyncio.as_completed(tasks):
        try:
            download_path = await task
            loop = asyncio.get_event_loop()
            executor = ThreadPoolExecutor()
            await loop.run_in_executor(executor, process_file, download_path)
        except DownloadError as e:
            print(f"Task {task} failed with error: {e}")


async def main():
    """
    Main asynchronous function to download and process Debian package files. 
    For test execution.
    """
    start = time.time()
    files = get_contents_file_list(MIRROR)
    files = process_contents_file_list(MIRROR, files)
    urls = filter_files(files, "arm64", True)
    await download_and_process_files(urls, "./downloads", 10)
    stats = return_stats(package_stats_dict, True, 20)
    print(stats)
    print("Time taken:", time.time()-start)


def package_stats(arch, mirror, include_udeb, limit, output_dir, skip_download):
    """
    Calculate and print package statistics based on given parameters.

    Args:
        arch (str): Architecture of the packages to parse.
        mirror (str): Mirror URL for contents files.
        include_udeb (bool): Flag to include udeb files for architecture.
        limit (int): Top 'limit' number of packages with maximum count of files.
        output_dir (str): Download location for content files.
        skip_download (int): Skip download if files are already present and newer than 's' days.

    """
    files = get_contents_file_list(mirror)
    files = process_contents_file_list(mirror, files)
    urls = filter_files(files, arch, include_udeb)
    asyncio.run(download_and_process_files(
        urls, output_dir, skip_download))
    stats = return_stats(package_stats_dict, True, limit)
    print(stats)


def cli():
    """
    Command-line interface function to get package statistics.
    """
    argparser = argparse.ArgumentParser(
        description="CLI tool to get the package statistics of debian packages given architecture."
    )
    argparser.add_argument(
        "architecture", type=str,
        help="Architecture of the packages to parse.")
    argparser.add_argument(
        "-m", "--mirror_url", type=str,
        default="http://ftp.uk.debian.org/debian/dists/stable/main/",
        help=(
            "Mirror URL for contents files. "
            "DEFAULT: http://ftp.uk.debian.org/debian/dists/stable/main/")
    )
    argparser.add_argument(
        "-u", "--udeb",
        help=("Include udeb file for architecture. \n"
              "DEFAULT: False"),
        action="store_true"
    )
    argparser.add_argument(
        "-l", "--limit", type=int, default=10,
        help=("Top 'l' number of packages with maximum count of files. \n"
              "DEFAULT: 10"
              )
    )
    argparser.add_argument(
        "-o", "--output-dir", type=str, default=os.path.join(os.getcwd(), "downloads"),
        help=(
            "Download location for content files \n"
            "DEFAULT: current-working-directory/downloads"
        )
    )
    argparser.add_argument(
        "-s", "--skip-download", type=int, default=0,
        help=(
            "Skip download if files are already present and newer than 's' days. \n"
            "DEFAULT: 10"
        ),
    )
    args = argparser.parse_args()
    package_stats(arch=args.architecture, mirror=args.mirror_url, include_udeb=args.udeb,
                  limit=args.limit, output_dir=args.output_dir, skip_download=args.skip_download)


if __name__ == "__main__":
    asyncio.run(main())
