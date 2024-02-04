from common_utils import *
from collections import defaultdict
import os
import asyncio
import aiohttp
import gzip
import time

BUF_SIZE = 1024
SEC_IN_DAY = 86400
MIRROR = "http://ftp.uk.debian.org/debian/dists/stable/main/"

def download_files(urls, output_dir, skip_download=None):
    output_paths = []
    try:
        for url in urls:
            file_name = os.path.basename(url)
            output_path = os.path.join(output_dir, file_name)
            if skip_download:
                if os.path.exists(output_path):
                    time_since_download = time.time() - os.path.getmtime(output_path)
                    if time_since_download < skip_download * SEC_IN_DAY:
                        # print("Found file. Skipping download")
                        output_paths.append(output_path)
                        continue
            response = requests.get(url, stream=True)  # Stream for large files

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
    with gzip.open(file_path, 'rb') as f:
        for chunk in f.readlines():
            unzipped_data = chunk.decode()
            yield unzipped_data


def process_data(line):
    line = line.strip()
    file_name, package_names = line.rsplit(maxsplit=1)
    package_names_list = package_names.split(",")
    if file_name != 'EMPTY_PACKAGE':
        return file_name, package_names_list
    return None, []


def download_and_process_files(files, arch, include_udeb, output_dir, skip_download):
    # package_stats_dict = defaultdict(list)
    package_stats_dict = defaultdict(int)
    urls, names = filter_files(files, arch, include_udeb)
    # print("urls", urls)
    download_paths = download_files(urls, output_dir, skip_download)

    for download_path in download_paths:
        for data in decompress_file(download_path):
            file_name, packages_list = process_data(data)
            for package in packages_list:
                # package_stats_dict[package].append(file_name)
                package_stats_dict[package] += 1
    return package_stats_dict


def main():
    start = time.time()
    files = get_contents_file_list(MIRROR)
    files = process_contents_file_list(MIRROR, files)
    # print("files", files)
    stats_dict = download_and_process_files(
        files, "amd64", True, "./downloads", 10)
    stats = return_stats(stats_dict, True, 20)
    print(stats)
    print(time.time() - start)


if __name__ == "__main__":
    main()
