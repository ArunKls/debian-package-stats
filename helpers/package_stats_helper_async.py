from common_utils import *
from collections import defaultdict
import os
import asyncio
import aiohttp
import aiofiles
import gzip
import time

PARTITION = 5000
SEC_IN_DAY = 86400
MIRROR = "http://ftp.uk.debian.org/debian/dists/stable/main/"
package_stats_dict = defaultdict(int)


async def download_and_process_file(url, output_dir, skip_download):
    download_path = await download_file(url, output_dir, skip_download)
    await process_file(download_path)


async def download_file(url, output_dir, skip_download):
    # print("Downloading file", url)
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
    # await asyncio.sleep(random.randint(0, 20))
    # print("Processing file", file_path)
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
    # print("Processed file", file_path)


async def mapper(lines):
    # await asyncio.sleep(random.randint(0, 20))
    # print("mapper", len(lines))
    for line in lines:
        line = line.strip()
        file_name, package_names = line.rsplit(maxsplit=1)
        package_names_list = package_names.split(",")
        if file_name == 'EMPTY_PACKAGE':
            return
        for package in package_names_list:
            package_stats_dict[package] += 1
    # print("mapper done")


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
    files = get_contents_file_list(MIRROR)
    files = process_contents_file_list(MIRROR, files)
    # print(files)
    await download_and_process_files(files, "amd64", True, "./downloads", 10)
    stats = return_stats(package_stats_dict, True, 20)
    print(stats)
    print("Time taken:", time.time()-start)

def package_stats(arch, mirror, include_udeb, limit, output_dir, skip_download):
    files = get_contents_file_list(mirror)
    files = process_contents_file_list(mirror, files)
    asyncio.run(download_and_process_files(files, arch, include_udeb, output_dir, skip_download))
    stats = return_stats(package_stats_dict, True, limit)
    print(stats)


def cli():
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
        help=("Include udeb file for architecture. "
              "DEFAULT: False"),
        action="store_true"
    )
    argparser.add_argument(
        "-l", "--limit", type=int, default=10,
        help=("Top l number of packages with maximum count of files.",
              "DEFAULT: 10"
              )
    )
    argparser.add_argument(
        "-o", "--output-dir", type=str, default=os.path.join(os.getcwd(), "downloads"),
        help=(
            "Download location for content files"
            "DEFAULT: current-working-directory/downloads"
        )
    )
    argparser.add_argument(
        "-s", "--skip-download", type=int, default=0,
        help=(
            "Skip download if files are already present and newer than s days",
            "DEFAULT: 10"
        ),
    )
    args = argparser.parse_args()
    package_stats(arch=args.architecture, mirror=args.mirror_url, include_udeb=args.udeb,
         limit=args.limit, output_dir=args.output_dir, skip_download=args.skip_download)

if __name__ == "__main__":
    asyncio.run(main())
