import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import os
import asyncio
import aiohttp
import aiofiles
import gzip
import time
import subprocess

PARTITION = 100000
SEC_IN_DAY = 86400
package_stats_dict = defaultdict(int)


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


async def download_and_process_file(url, output_dir, skip_download):
    download_path = await download_file(url, output_dir, skip_download)
    await process_file(download_path)


async def download_file(url, output_dir, skip_download):
    print("Downloading file", url)
    file_name = os.path.basename(url)
    output_path = os.path.join(output_dir, file_name)
    if skip_download:
        if os.path.exists(output_path):
            time_since_download = time.time() - os.path.getmtime(output_path)
            if time_since_download < skip_download * SEC_IN_DAY:
                # print("Found file. Skipping download")
                return output_path
    async with aiohttp.ClientSession() as session:
        async with session.get(url) as response:
            if response.status == 200:
                filename = os.path.basename(url)
                output_path = os.path.join(output_dir, filename)

                async with aiofiles.open(output_path, 'wb') as f:
                    await f.write(await response.read())
                print("Downloaded file", output_path)
                return output_path

            else:
                print(f"Error fetching {url}: status code {response.status}")
                return None


async def process_file(file_path):
    print("Processing file", file_path)
    tasks = []
    with gzip.open(file_path, 'rb') as f:
        buffer = []
        for line in f.readlines():
            buffer.append(line.decode())
            if len(buffer) < PARTITION:
                continue
            else:
                # print(len(buffer))
                tasks.append(mapper(buffer))
                buffer = []
                # yield task
        # print("buffer left", len(buffer))
        tasks.append(mapper(buffer))
    await asyncio.gather(*tasks)
    print("Processed file", file_path)


async def mapper(lines):
    print("mapper", len(lines))
    for line in lines:
        line = line.strip()
        file_name, package_names = line.rsplit(maxsplit=1)
        package_names_list = package_names.split(",")
        if file_name == 'EMPTY_PACKAGE':
            return
        for package in package_names_list:
            package_stats_dict[package] += 1
    print("mapper done")


def filter_files(files, arch, include_udeb):
    urls = []
    names = []
    # print(files.keys(), arch in files.keys())
    # for file in files[arch]:
    #     if include_udeb or (not file.get("udeb")):
    #         urls.append(file.get("link"))
    #         names.append(file.get("name"))
    for arch in files.keys():
        for file in files[arch]:
            urls.append(file.get("link"))
            names.append(file.get("name"))
    return urls, names


def return_stats(package_stats, descending, count):
    sorted_stats = sorted(package_stats.items(),
                          key=lambda x: x[1], reverse=descending)
    output = [f"{'Package':50} \t File Count"]
    for line in range(min(count, len(sorted_stats))):
        output.append(f"{sorted_stats[line][0]:50} \t {sorted_stats[line][1]}")
    return "\n".join(output)


async def download_and_process_files(files, arch, include_udeb, output_dir, skip_download):
    # package_stats_dict = defaultdict(list)
    urls, names = filter_files(files, arch, include_udeb)
    # print("urls", urls)
    tasks = []
    for url in urls:
        tasks.append(download_and_process_file(url, output_dir, skip_download))
    await asyncio.gather(*tasks)


async def main():
    start = time.time()
    files = get_contents_file_list(
        "http://ftp.uk.debian.org/debian/dists/stable/main/")
    files = process_contents_file_list(
        "http://ftp.uk.debian.org/debian/dists/stable/main/", files)
    # print(files)
    await download_and_process_files(files, "amd64", True, "./downloads", 0)
    stats = return_stats(package_stats_dict, True, 20)
    print(stats)
    print(time.time()-start)


if __name__ == "__main__":
    asyncio.run(main())
