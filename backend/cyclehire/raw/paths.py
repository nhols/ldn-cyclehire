import urllib.parse
from pathlib import Path


BUCKET = "cycling.data.tfl.gov.uk"
S3_HOST = "https://s3-eu-west-1.amazonaws.com"
PREFIX = "usage-stats/"
LISTING_URL = (
    f"{S3_HOST}/{BUCKET}/?list-type=2&delimiter=/&prefix={urllib.parse.quote(PREFIX)}"
)
PUBLIC_BASE_URL = f"https://{BUCKET}/"


def public_url_for(source_key: str) -> str:
    return urllib.parse.urljoin(PUBLIC_BASE_URL, urllib.parse.quote(source_key))


def source_relative_path(source_key: str) -> Path:
    return Path(*source_key.split("/"))


def raw_path_for(source_key: str) -> Path:
    return Path("raw") / source_relative_path(source_key)
