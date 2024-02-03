import requests
from bs4 import BeautifulSoup
from collections import defaultdict
import os
import asyncio
import aiohttp
import aiofiles
import gzip
import time


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


async def download_and_process_file(url, output_dir):
    download_path = await download_file(url, output_dir)
    if download_path:
        await decompress_file(download_path)
        # async for data in decompress_file(download_path):
        #     await process_data(data)


async def download_file(url, output_dir):
    print("Downloading file", url)
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


async def decompress_file(file_path):
    print("Decompressing file", file_path)
    await asyncio.sleep(1)
    print("Decompressed file", file_path)
    # async with gzip.open(file_path, 'rb') as f:
    #     async for chunk in f.readlines():
    #         unzipped_data = chunk.decode()
    #         yield unzipped_data  # Yield each chunk of decompressed data


async def process_data(line):
    pass


def filter_files(files, arch, include_udeb):
    urls = []
    names = []
    # print(files.keys(), arch in files.keys())
    for file in files[arch]:
        if include_udeb or (not file.get("udeb")):
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
    package_stats_dict = defaultdict(int)
    urls, names = filter_files(files, arch, include_udeb)
    print("urls", urls)
    tasks = []
    for url in urls:
        tasks.append(download_and_process_file(url, output_dir))
    await asyncio.gather(*tasks)


async def main():
    files = get_contents_file_list(
        "http://ftp.uk.debian.org/debian/dists/stable/main/")
    files = process_contents_file_list(
        "http://ftp.uk.debian.org/debian/dists/stable/main/", files)
    # print(files)
    await download_and_process_files(files, "amd64", True, "./downloads", False)


if __name__ == "__main__":
    asyncio.run(main())
