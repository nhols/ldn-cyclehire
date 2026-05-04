import logging
import urllib.request
import xml.etree.ElementTree as ET
from dataclasses import dataclass
from pathlib import Path

from cyclehire.raw.paths import LISTING_URL, PREFIX, public_url_for, source_relative_path


LOGGER = logging.getLogger(__name__)


@dataclass(frozen=True)
class SourceObject:
    key: str
    size_bytes: int
    last_modified: str
    etag: str | None

    @property
    def url(self) -> str:
        return public_url_for(self.key)

    @property
    def local_relative_path(self) -> Path:
        return source_relative_path(self.key)


def fetch_source_listing() -> list[SourceObject]:
    LOGGER.info("Fetching TfL source listing: %s", LISTING_URL)
    with urllib.request.urlopen(LISTING_URL, timeout=60) as response:
        xml_bytes = response.read()

    root = ET.fromstring(xml_bytes)
    namespace = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    objects: list[SourceObject] = []
    for contents in root.findall("s3:Contents", namespace):
        key = _required_text(contents, "s3:Key", namespace)
        if key == PREFIX:
            continue
        objects.append(
            SourceObject(
                key=key,
                size_bytes=int(_required_text(contents, "s3:Size", namespace)),
                last_modified=_required_text(contents, "s3:LastModified", namespace),
                etag=_optional_text(contents, "s3:ETag", namespace),
            )
        )
    return objects


def _required_text(element: ET.Element, path: str, namespace: dict[str, str]) -> str:
    value = _optional_text(element, path, namespace)
    if value is None:
        raise ValueError(f"Missing required XML element: {path}")
    return value


def _optional_text(element: ET.Element, path: str, namespace: dict[str, str]) -> str | None:
    found = element.find(path, namespace)
    if found is None or found.text is None:
        return None
    return found.text.strip().strip('"')
