###################################################################
"""
Main entry point for the debian package stats module

$ python3 package_statistics.py --help
usage: package_statistics.py [-h] [-m MIRROR_URL] [-u] [-l LIMIT]
                             [-o OUTPUT_DIR] [-s SKIP_DOWNLOAD]
                             architecture

CLI tool to get the package statistics of debian packages given architecture.

positional arguments:
  architecture          Architecture of the packages to parse.

options:
  -h, --help            show this help message and exit
  -m MIRROR_URL, --mirror_url MIRROR_URL
                        Mirror URL for contents files. DEFAULT:
                        http://ftp.uk.debian.org/debian/dists/stable/main/
  -u, --udeb            Include udeb file for architecture. DEFAULT: False
  -l LIMIT, --limit LIMIT
                        Top 'l' number of packages with maximum count of
                        files. DEFAULT: 10
  -o OUTPUT_DIR, --output-dir OUTPUT_DIR
                        Download location for content files DEFAULT: current-
                        working-directory/downloads
  -s SKIP_DOWNLOAD, --skip-download SKIP_DOWNLOAD
                        Skip download if files are already present and newer
                        than 's' days. DEFAULT: 10
"""
###################################################################

from helpers.package_stats_helper_async import cli

if __name__ == "__main__":
    cli()
