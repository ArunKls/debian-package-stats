import requests
from bs4 import BeautifulSoup
import os
from collections import defaultdict
import argparse


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


def filter_files(files, arch, include_udeb):
    urls = []
    names = []
    # print(files.keys(), arch in files.keys())
    for file in files[arch]:
        if include_udeb or (not file.get("udeb")):
            urls.append(file.get("link"))
            names.append(file.get("name"))
    # for arch in files.keys():
    #     for file in files[arch]:
    #         urls.append(file.get("link"))
    #         names.append(file.get("name"))
    return urls, names


def return_stats(package_stats, descending, count):
    sorted_stats = sorted(package_stats.items(),
                          key=lambda x: x[1], reverse=descending)
    output = [f"{'Package':50} \t File Count"]
    for line in range(min(count, len(sorted_stats))):
        output.append(f"{sorted_stats[line][0]:50} \t {sorted_stats[line][1]}")
    return "\n".join(output)

