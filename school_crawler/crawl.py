"""
Crawls K12 websites in the SQLite database, based on the school_info table,
breadth-first to avoid overloading any one site.

Usage: python crawl.py <db_path>

"""
import aiosqlite
import asyncio
import logging
from urllib.parse import urlparse
import cached_http_client
import sys
import traceback


CRAWL_QUEUE_TABLE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS crawl_queue (
        queue_id INTEGER PRIMARY KEY,
        school_name TEXT,
        base_hostname TEXT,
        depth INTEGER NOT NULL DEFAULT 0,
        url_to_visit TEXT,
        result_webpage_id TEXT,
        referral_queue_id INTEGER
    );

    CREATE INDEX IF NOT EXISTS crawl_queue_base_hostname ON crawl_queue (base_hostname);
    CREATE INDEX IF NOT EXISTS crawl_queue_result_webpage_id ON crawl_queue (result_webpage_id);

    -- We need to make sure we don't have duplicate (school_name, url_to_visit) pairs
    CREATE UNIQUE INDEX IF NOT EXISTS crawl_queue_school_url ON crawl_queue (school_name, url_to_visit);

"""


NUMBER_OF_WORKERS = 50
number_of_random_base_hostnames = 10 * NUMBER_OF_WORKERS


logging.basicConfig(
    format='%(asctime)s - %(levelname)s - %(message)s',
    datefmt='%m/%d/%Y %I:%M:%S %p',
    filename='crawl.log',
    filemode='w',
    level=logging.INFO
)


db_write_lock = None


async def main():

    try:
        db_path = sys.argv[1]
    except IndexError:
        print("Usage: python crawl.py <db_path>")
        sys.exit(1)

    # Need to intialize the lock within the event loop; can't do it at the
    # global level before the event loop is initialized; see
    # https://chat.openai.com/share/ab2495d7-ce07-4f69-83e8-52d825beefdd
    global db_write_lock
    db_write_lock = asyncio.Lock()

    # Create the database connection
    db_conn = await aiosqlite.connect(db_path, isolation_level=None)

    # Enable the WAL mode for better concurrency
    await db_conn.execute('PRAGMA journal_mode=WAL;')

    # Set a higher cache size to reduce disk writes
    await db_conn.execute('PRAGMA cache_size=-128000;')

    # Initialize custom stuff
    await initialize_udfs(db_conn)
    await initialize_views(db_conn)

    # Initialize the crawl queue
    await initialize_crawl_queue(db_conn)

    # Create the HTTP client
    http_client = cached_http_client.CachedHTTPClient()
    await http_client.initialize(db_conn, db_write_lock)

    # Start the crawl
    await crawl(db_conn, http_client)

    # Close the database connection
    await db_conn.close()



async def initialize_udfs(db_conn):
    """Defines various UDFs for later use."""

    await db_conn.create_function('get_hostname_from_url', 1, get_hostname_from_url)

    await db_conn.create_function('is_valid_url', 1, cached_http_client.is_valid_url)



async def initialize_views(db_conn):
    """Initializes the views in the database."""

    q = f"""
        -- Select a small number of hostnames
        CREATE TEMPORARY VIEW IF NOT EXISTS random_base_hostnames AS
        SELECT base_hostname
        FROM crawl_queue
        GROUP BY base_hostname
        ORDER BY RANDOM()
        LIMIT {number_of_random_base_hostnames};

        -- Get the first unvisited URL from each hostname
        CREATE TEMPORARY VIEW IF NOT EXISTS random_base_hostname_with_min_queue_id AS
        SELECT
            a.base_hostname AS base_hostname,
            min(b.queue_id) as min_queue_id
        FROM random_base_hostnames a
        JOIN crawl_queue b
        ON a.base_hostname = b.base_hostname
        WHERE b.result_webpage_id IS NULL
        GROUP BY a.base_hostname;
    """
    async with db_write_lock:
        await db_conn.executescript(q)



async def initialize_crawl_queue(db_conn):
    """
    Initializes the crawl queue table by filling in initial data from
    school_info.

    """
    # Initialize the database
    db_conn.row_factory = aiosqlite.Row
    await db_conn.executescript(CRAWL_QUEUE_TABLE_SCHEMA)

    # Count the number of existing rows in the crawl queue
    q = "SELECT COUNT(*) AS row_count FROM crawl_queue"
    row = await db_fetch_one(db_conn, q)
    if row['row_count'] > 0:
        return

    # Since the crawl queue is empty, fill it with the initial data from
    # school_info
    q = """
        INSERT INTO crawl_queue (school_name, base_hostname, url_to_visit, depth)
        SELECT school_name, get_hostname_from_url(actual_website), actual_website, 0
        FROM school_info
        WHERE actual_website_status = 200
        GROUP BY school_name, actual_website
        HAVING is_valid_url(actual_website);
    """
    async with db_write_lock:
        await db_conn.execute(q)



async def crawl(db_conn, http_client):
    """
    Crawls the webpages in the crawl queue.

    """
    tasks = []

    temp_queue = asyncio.Queue()
    queue_lock = asyncio.Lock()

    for worker_id in range(NUMBER_OF_WORKERS):
        task = asyncio.create_task(worker(worker_id, http_client, db_conn, temp_queue, queue_lock))
        tasks.append(task)

    await asyncio.gather(*tasks, return_exceptions=False)



async def worker(worker_id, http_client, db_conn, temp_queue, queue_lock):

    logging.info(f'DEBUG: Starting worker {worker_id}')

    while True:

        try:
            await worker_helper(worker_id, http_client, db_conn, temp_queue, queue_lock)

        except Exception as e:
            logging.exception(f'ERROR: Worker {worker_id} encountered an exception: {e} - Traceback: {traceback.format_exc()}')
            await asyncio.sleep(5)



async def worker_helper(worker_id, http_client, db_conn, temp_queue, queue_lock):

    # Get 100 unvisited URLs from 100 random base_hostnames and stick them into a temporary queue
    async with queue_lock:

        if temp_queue.empty():

            logging.info(f'DEBUG: Worker {worker_id} is fetching {number_of_random_base_hostnames} URLs')

            # Breadth-first search based on the base_hostname to avoid crawling a
            # single base_hostname too often too fast
            q = """
                SELECT
                    c.queue_id AS queue_id,
                    c.school_name AS school_name,
                    c.base_hostname AS base_hostname,
                    c.depth AS depth,
                    c.url_to_visit AS url_to_visit,
                    c.result_webpage_id AS result_webpage_id,
                    c.referral_queue_id AS referral_queue_id
                FROM crawl_queue c
                JOIN random_base_hostname_with_min_queue_id r
                ON c.queue_id = r.min_queue_id;
            """
            async with db_conn.execute(q) as cursor:
                async for row in cursor:
                    await temp_queue.put(row)

            qsize = temp_queue.qsize()
            logging.info(f'DEBUG: Worker {worker_id} just added {qsize} URLs to the temporary queue')

        row = await temp_queue.get()

    # Process the URLs in the temporary queue
    await process_crawl_queue_row(row, http_client, db_conn)

    # Report statistics
    if worker_id == 0:
        stat_dict = await http_client.get_statistics()
        if stat_dict is not None:
            logging.info(f'DEBUG: Statistics: {stat_dict}')



async def process_crawl_queue_row(row, http_client, db_conn):

    if not cached_http_client.is_valid_url(row['url_to_visit']):
        return

    try:
        # Visit the URL
        (webpage_id, redirected_url, href_list) = await http_client.visit_url(row['url_to_visit'])

    except cached_http_client.InvalidURL:
        # Remove the URL from queue
        q = "DELETE FROM crawl_queue WHERE queue_id = ?"
        async with db_write_lock:
            await db_conn.execute(q, (row['queue_id'],))
        return

    # Update the crawl queue with the result
    q = """
        UPDATE crawl_queue
        SET result_webpage_id = ?
        WHERE queue_id = ?;
    """
    async with db_write_lock:
        await db_conn.execute(q, (webpage_id, row['queue_id']))

    # Return upon error
    if redirected_url == '':
        return

    # If the page visited is already a third party (i.e., with a different
    # hostname from the base_hostname), we don't want to add its href links to
    # the crawl queue. We stop here.
    redirected_url_hostname = get_hostname_from_url(redirected_url)
    if redirected_url_hostname != row['base_hostname']:
        return

    # Remove the invalid URLs
    href_list = [href for href in href_list if cached_http_client.is_valid_url(href)]

    args = [
        (
            row['school_name'],
            row['base_hostname'],
            row['depth'] + 1,
            href,
            row['queue_id'],
        )
        for href in href_list
    ]

    if len(args) > 0:

        # Insert the new URLs into the crawl queue
        q = """
            INSERT OR IGNORE INTO crawl_queue (
                school_name,
                base_hostname,
                depth,
                url_to_visit,
                referral_queue_id
            )
            VALUES (?, ?, ?, ?, ?);
        """
        async with db_write_lock:
            await db_conn.executemany(q, args)



def get_hostname_from_url(url) -> str:
    """
    Returns the hostname from the given URL. Used as a UDF.

    """
    try:
        hostname = urlparse(url).hostname
    except Exception:
        hostname = ''

    if hostname is None:
        hostname = ''

    return hostname.strip().lower()



async def db_fetch_one(db_conn, q, args=[]):
    async with db_conn.execute(q, args) as cursor:
        row = await cursor.fetchone()

    return row



if __name__ == '__main__':
    logging.basicConfig(level=logging.INFO)
    asyncio.run(main())