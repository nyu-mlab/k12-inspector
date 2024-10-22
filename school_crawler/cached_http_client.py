"""
An HTTP client that caches and parses responses.

Usage:
    - Initialize the HTTP client with a database connection
    - Visit a URL with the visit_url method

"""
import httpx
import json
import time
from bs4 import BeautifulSoup
from urllib.parse import urljoin
import aiosqlite
import uuid
from functools import lru_cache



HTTP_CLIENT_CONFIG = {
    'follow_redirects': True,
    'headers': {
        'Referer': 'https://www.google.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
}



BINARY_CONTENT_TYPES = [
    'pdf', 'zip', 'audio', 'image', 'video', 'octet-stream'
]


BINARY_CONTENT_EXTENSIONS = [
    'gif', 'jpg', 'png',
    'doc', 'docx', 'xls', 'xlsx', 'pdf',
    'mp4', 'mp3', 'mov', 'flv'
]


# Create the initial table structure
WEBPAGE_TABLE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS webpage (
        webpage_id TEXT PRIMARY KEY,
        url_to_visit TEXT NOT NULL DEFAULT '',
        redirected_url TEXT NOT NULL DEFAULT '',
        visit_ts INTEGER NOT NULL DEFAULT 0,
        status TEXT NOT NULL DEFAULT '',
        raw_html TEXT NOT NULL DEFAULT '',
        title TEXT NOT NULL DEFAULT '',
        text TEXT NOT NULL DEFAULT '',
        href_list_json TEXT NOT NULL DEFAULT '[]',
        metadata_dict_json TEXT NOT NULL DEFAULT '{}'
    );

    CREATE INDEX IF NOT EXISTS webpage_webpage_id ON webpage (webpage_id);
    CREATE INDEX IF NOT EXISTS webpage_url_to_visit ON webpage (url_to_visit);
    CREATE INDEX IF NOT EXISTS webpage_redirected_url ON webpage (redirected_url);
    CREATE INDEX IF NOT EXISTS webpage_visit_ts ON webpage (visit_ts);

"""

# A view for statistical reporting purpose
WEBPAGE_STATISTICS_VIEW_SCHEMA = """
    CREATE VIEW IF NOT EXISTS webpage_statistics AS
    SELECT
        webpage_id,
        CASE
            WHEN redirected_url = '' THEN 'error'
            ELSE 'success'
        END AS visit_status,
        visit_ts
    FROM webpage
    WHERE visit_ts > strftime('%s', 'now') - 15;
"""


class CachedHTTPClient(object):

    async def initialize(self, db_conn, db_write_lock):

        # When we last reported statistics
        self._last_reported_statistics_ts = 0

        # Initialize the database
        self._db = db_conn
        self._db.row_factory = aiosqlite.Row
        self._db_write_lock = db_write_lock

        async with self._db_write_lock:
            await self._db.executescript(WEBPAGE_TABLE_SCHEMA)
            await self._db.executescript(WEBPAGE_STATISTICS_VIEW_SCHEMA)

        # Create the HTTP client
        self._client = httpx.AsyncClient(**HTTP_CLIENT_CONFIG)

    async def visit_url(self, url):
        """
        Returns (webpage_id, redirected_url, href_list) for the given URL.

        Raises InvalidURL if the URL is invalid.

        """
        # Check if the URL is valid
        if not is_valid_url(url):
            raise InvalidURL(f'Invalid URL: {url}')

        # Check if we have already visited this URL
        q = """
            SELECT webpage_id, redirected_url, href_list_json FROM webpage
            WHERE
                url_to_visit = ? OR
                redirected_url = ?
            LIMIT 1;
        """
        async with self._db.execute(q, (url, url)) as cursor:
            row = await cursor.fetchone()
            if row:
                return (
                    row['webpage_id'],
                    row['redirected_url'],
                    json.loads(row['href_list_json'])
                )

        # Generate a unique webpage_id
        webpage_id = str(uuid.uuid4()).replace('-', '')

        # Visit the URL
        try:
            resp = await self._client.get(url)
        except Exception as e:
            status = 'Error: ' + str(e)
            q = """
                INSERT INTO webpage (webpage_id, url_to_visit, status, visit_ts)
                VALUES (?, ?, ?, ?);
            """
            async with self._db_write_lock:
                await self._db.execute(q, (webpage_id, url, status, int(time.time())))
            return (webpage_id, '', [])

        # Get the redirected URL
        redirected_url = str(resp.url)

        # Check if the page has binary contents
        content_type = resp.headers.get('Content-Type', '').lower()
        is_binary_content = False
        for binary_content_type in BINARY_CONTENT_TYPES:
            if binary_content_type in content_type:
                is_binary_content = True
                break

        # Initialize variables
        raw_html = ''
        text = ''
        title = ''
        href_list = []
        metadata = {}

        # Parse the response if not binary
        if not is_binary_content:

            # Get the raw html from the response
            raw_html = resp.text.strip()

            # Parse with BeautifulSoup
            soup = BeautifulSoup(raw_html, 'html.parser')

            # Extract the page title
            try:
                title = soup.find('title').get_text().strip()
            except Exception:
                title = ''

            # Extract the text, ignoring the html tags
            text = soup.get_text(separator=' ', strip=True)

            # Extract the hrefs, constructing the full URL rather than the
            # relative URL
            href_set = set()
            for a in soup.find_all('a'):
                href = a.get('href')
                if href:
                    try:
                        href = urljoin(redirected_url, href)
                    except ValueError:
                        continue
                    if is_valid_url(href):
                        href_set.add(href)
            href_list = list(href_set)

            # Extract the metadata
            for meta in soup.find_all('meta'):
                name = meta.get('name')
                content = meta.get('content')
                if name and content:
                    metadata[name] = content

        # Insert the webpage into the database
        q = """
            INSERT INTO webpage (
                webpage_id,
                url_to_visit, redirected_url, visit_ts, status,
                raw_html, title, text, href_list_json, metadata_dict_json)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?);
        """
        async with self._db_write_lock:
            await self._db.execute(q, (
                webpage_id,
                url, redirected_url, int(time.time()), resp.status_code,
                raw_html[0:10000000], title, text, json.dumps(href_list), json.dumps(metadata)
            ))

        return (
            webpage_id,
            redirected_url,
            href_list
        )

    async def get_statistics(self, callable_every_n_seconds=10):
        """
        Returns a dictionary of the number of pages visited (with and without
        errors) per second in the past 15 seconds. The keys are 'error' and
        'success'. The values are the number of webpages visited per second
        given each key.

        This method can be called every `callable_every_n_seconds` seconds. If
        called more frequently, returns None.

        """
        if time.time() - self._last_reported_statistics_ts < callable_every_n_seconds:
            return None

        q = """
            SELECT
                visit_status,
                COUNT(webpage_id) as count
            FROM webpage_statistics
            GROUP BY visit_status;
        """
        ret_dict = {}
        async with self._db.execute(q) as cursor:
            async for row in cursor:
                ret_dict[row['visit_status']] = round(row['count'] / 15.0, 1)

        self._last_reported_statistics_ts = time.time()
        return ret_dict

    def close(self):
        self._client.close()



class InvalidURL(Exception):
    pass


@lru_cache(maxsize=1024*128)
def is_valid_url(url):

    if not isinstance(url, str):
        return False

    if not url.startswith('http'):
        return False

    url_lower = url.lower()
    for extension in BINARY_CONTENT_EXTENSIONS:
        if f'.{extension}' in url_lower:
            return False

    return True


async def test():

    import aiosqlite

    async with aiosqlite.connect('test.sqlite', isolation_level=None) as db:

        client = CachedHTTPClient()
        await client.initialize(db)

        # A list of test URLs
        test_url_list = [
            'http://hdanny.org',
            'https://hdanny.org/static/chi23-k12.pdf?dfdsf',
            'https://hdanny.org/sfsfsdfdsfdsfdsf',
            'https://drive.usercontent.google.com/u/1/uc?id=1R74BO-FQPSxDaViDiS2Aiwmz_p071bPO&export=download',
            'https://hdanny.org/sfsfsdfdsfdsfdsf',
            'https://hdanny.org',
            'https://hdanny.org/',
            'https://dfasfsdfsfsafsdfsdfsdf.com'
        ]

        # Visit the test URLs
        for url in test_url_list:
            try:
                hrefs = await client.visit_url(url)
                print(f'Number of hrefs for {url}: {len(hrefs)}')
            except InvalidURL as e:
                print(f'Invalid URL: {url}')



if __name__ == '__main__':
    import asyncio
    asyncio.run(test())