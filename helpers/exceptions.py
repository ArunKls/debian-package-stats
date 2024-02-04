"""Custom Exceptions"""


class DownloadError(Exception):
    """Custom exception for download failures."""
    def __init__(self, url, status_code=""):
        self.url = url
        self.status_code = status_code
        super().__init__(f"Download from {url} failed with status code {status_code}")