import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import os
import asyncio
import aiohttp
import gzip
import time

BUF_SIZE = 1024
SEC_IN_DAY = 86400


def get_contents_file_list(url):
    try:
        response = requests.get(url)
        response.raise_for_status()  # Check for errors in the HTTP response

        soup = BeautifulSoup(response.content, 'html.parser')

        file_links = soup.find_all('a', href=True)

    except requests.exceptions.RequestException as e:
        print(f"Error: {e}")
    return file_links


def process_contents_file_list(url, file_links):
    files = defaultdict(list)
    for link in file_links:
        file_url = link['href']
        if file_url.endswith(".gz"):
            file_name = link.text
            file_name_wo_ext, ext = os.path.splitext(file_name)
            file_name_split = file_name_wo_ext.split("-")
            arch = file_name_split[-1]
            files[arch].append(
                {
                    "name": link['href'],
                    "link": os.path.join(url, link.text),
                    "udeb": "udeb" in file_name_split
                }
            )
    return files


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


def filter_files(files, arch, include_udeb):
    urls = []
    names = []
    # print(files.keys(), arch in files.keys())
    for file in files[arch]:
        if include_udeb or (not file.get("udeb")):
            urls.append(file.get("link"))
            names.append(file.get("name"))
    return urls, names


def download_and_process_files(files, arch, include_udeb, output_dir, skip_download):
    # package_stats_dict = defaultdict(list)
    package_stats_dict = defaultdict(int)
    urls, names = filter_files(files, arch, include_udeb)
    print("urls", urls)
    download_paths = download_files(urls, output_dir, skip_download)

    for download_path in download_paths:
        for data in decompress_file(download_path):
            file_name, packages_list = process_data(data)
            for package in packages_list:
                # package_stats_dict[package].append(file_name)
                package_stats_dict[package] += 1
    return package_stats_dict


def return_stats(package_stats, descending, count):
    sorted_stats = sorted(package_stats.items(),
                          key=lambda x: x[1], reverse=descending)
    output = [f"{'Package':50} \t File Count"]
    for line in range(min(count, len(sorted_stats))):
        output.append(f"{sorted_stats[line][0]:50} \t {sorted_stats[line][1]}")
    return "\n".join(output)


def main():
    files = get_contents_file_list(
        "http://ftp.uk.debian.org/debian/dists/stable/main/")
    files = process_contents_file_list(
        "http://ftp.uk.debian.org/debian/dists/stable/main/", files)
    # print("files", files)
    stats_dict = download_and_process_files(
        files, "mipsel", True, "./downloads", 10)
    stats = return_stats(stats_dict, True, 20)
    print(stats)


if __name__ == "__main__":
    main()
