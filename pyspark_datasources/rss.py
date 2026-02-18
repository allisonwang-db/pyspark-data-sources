"""RSS data source - reads RSS/Atom feed items."""

import re
import xml.etree.ElementTree as ET

import requests
from pyspark.sql import Row
from pyspark.sql.datasource import DataSource, DataSourceReader

DEFAULT_FEED = "https://hnrss.org/frontpage"


def _text(elem):
    """Get text from element, handling CDATA."""
    if elem is None:
        return ""
    text = elem.text or ""
    for child in elem:
        text += child.tail or ""
    return text.strip()


def _strip_html(text):
    """Remove HTML tags from text."""
    return re.sub(r"<[^>]+>", "", text)[:500] if text else ""


class RssDataSource(DataSource):
    """
    A DataSource for reading RSS/Atom feed items.

    No API key required. Path specifies feed URL.

    Name: `rss`

    Schema: `title string, link string, description string, pub_date string, guid string`

    Examples
    --------
    Register the data source.

    >>> from pyspark_datasources import RssDataSource
    >>> spark.dataSource.register(RssDataSource)

    Load Hacker News RSS (default).

    >>> spark.read.format("rss").load().show()

    Load custom feed.

    >>> spark.read.format("rss").load("https://news.ycombinator.com/rss").show()
    """

    @classmethod
    def name(cls):
        return "rss"

    def schema(self):
        return (
            "title string, link string, description string, pub_date string, guid string"
        )

    def reader(self, schema):
        return RssReader(self.options)


class RssReader(DataSourceReader):
    def __init__(self, options):
        self.options = options
        self.feed_url = self.options.get("path") or DEFAULT_FEED

    def read(self, partition):
        try:
            response = requests.get(self.feed_url, timeout=30)
            response.raise_for_status()
            root = ET.fromstring(response.content)

            # Handle RSS 2.0 and Atom namespace
            items = root.findall(".//item") or root.findall(
                ".//{http://www.w3.org/2005/Atom}entry"
            )

            for item in items:
                ns = {"dc": "http://purl.org/dc/elements/1.1/"}
                title_el = item.find("title") or item.find(
                    "{http://www.w3.org/2005/Atom}title"
                )
                link_el = item.find("link") or item.find(
                    "{http://www.w3.org/2005/Atom}link"
                )
                desc_el = item.find("description") or item.find(
                    "{http://www.w3.org/2005/Atom}summary"
                )
                pub_el = (
                    item.find("pubDate")
                    or item.find("dc:date", ns)
                    or item.find("{http://www.w3.org/2005/Atom}updated")
                )
                guid_el = item.find("guid") or item.find(
                    "{http://www.w3.org/2005/Atom}id"
                )

                link = ""
                if link_el is not None:
                    link = link_el.get("href", _text(link_el))

                yield Row(
                    title=_strip_html(_text(title_el)),
                    link=link,
                    description=_strip_html(_text(desc_el)),
                    pub_date=_text(pub_el),
                    guid=_text(guid_el),
                )
        except (requests.RequestException, ET.ParseError):
            return iter([])
