"""
Usage: python analyze_third_parties.py <crawl_db_path> <analysis_output_db_path>

This script analyzes the third-party content found in the crawl database
(specified in `crawl_db_path`) and writes the results to a new database
(specified in `analysis_output_db_path`).

This script goes through all the url_to_visit in the crawl_queue (from the
`crawl_db`), extracts the unique hostnames, and inserts these hostnames into the
`hostname` table specified in the `analysis_output_db`.

"""
import dns.resolver # type: ignore
import asyncio
import aiosqlite
import sys
from functools import wraps
from crawl import get_hostname_from_url
import traceback


DNS_SERVER = '1.1.1.1'


NUMBER_OF_CONCURRENT_WORKERS = 10


HOSTNAME_TABLE_SCHEMA = """

    CREATE TABLE IF NOT EXISTS hostname (
        school_name TEXT,
        base_hostname TEXT,
        service_hostname TEXT
    );

    CREATE TABLE IF NOT EXISTS hostname_dns_info (
        hostname TEXT,
        dns_info TEXT,
        dns_info_type TEXT
    );

    CREATE INDEX IF NOT EXISTS hostname_service_hostname ON hostname (service_hostname);
    CREATE INDEX IF NOT EXISTS hostname_dns_info_hostname ON hostname_dns_info (hostname);

"""



async def main():

    try:
        crawl_db_path = sys.argv[1]
        analysis_db_path = sys.argv[2]
    except IndexError:
        print("Usage: python analyze_third_parties.py <crawl_db_path> <analysis_output_db_path>")
        sys.exit(1)

    # Make sure that the crawl_db_path exists
    try:
        with open(crawl_db_path, 'r'):
            pass
    except FileNotFoundError:
        print(f"File not found: {crawl_db_path}")
        sys.exit(1)

    (reader_conn, writer_conn) = await initialize_dbs(crawl_db_path, analysis_db_path)
    writer_lock = asyncio.Lock()

    # Reset the hostname_dns_info table
    q = """DELETE FROM hostname_dns_info;"""
    await writer_conn.execute(q)

    # Fill in DNS info
    task_set = set()
    q = """
        SELECT service_hostname
        FROM hostname
        WHERE service_hostname <> '' AND service_hostname IS NOT NULL
        GROUP BY service_hostname;
    """
    async with writer_conn.execute(q) as cursor:
        async for row in cursor:
            service_hostname = row['service_hostname']
            task = asyncio.create_task(get_hostname_info(service_hostname, writer_conn, writer_lock))
            task.add_done_callback(task_set.discard)
            task_set.add(task)

            while len(task_set) >= NUMBER_OF_CONCURRENT_WORKERS:
                await asyncio.sleep(0.1)

    # Cleaning up
    await asyncio.gather(*task_set)
    await reader_conn.close()
    await writer_conn.close()





async def initialize_dbs(crawl_db_path, analysis_db_path):

    # Initialize db connections
    reader_conn = await aiosqlite.connect(crawl_db_path, isolation_level=None)
    writer_conn = await aiosqlite.connect(analysis_db_path, isolation_level=None)
    await reader_conn.execute('PRAGMA journal_mode=WAL;')
    await writer_conn.execute('PRAGMA journal_mode=WAL;')

    # Use row factory for both connections
    reader_conn.row_factory = aiosqlite.Row
    writer_conn.row_factory = aiosqlite.Row

    # Initialize the UDF
    await reader_conn.create_function('get_hostname_from_url', 1, get_hostname_from_url)

    # Create the initial schema
    await writer_conn.executescript(HOSTNAME_TABLE_SCHEMA)

    # Count the number of rows in the hostname table
    async with writer_conn.execute('SELECT COUNT(*) FROM hostname') as cursor:
        row = await cursor.fetchone()
        num_rows = row[0]

    # Pull values from the crawl database if the hostname table is not initialized yet
    if num_rows == 0:
        q = f"""
            ATTACH DATABASE '{analysis_db_path}' AS write_db;
            PRAGMA write_db.journal_mode=WAL;

            -- Get the actual URLs visited if available
            CREATE TEMPORARY VIEW IF NOT EXISTS crawl_queue_with_actual_urls AS
            SELECT
                school_name,
                base_hostname,
                c.url_to_visit AS url_to_visit,
                w.redirected_url AS redirected_url
            FROM crawl_queue c
            LEFT JOIN webpage w ON c.result_webpage_id = w.webpage_id;

            -- Combine both URLs into a single URL, preferring the redirected_url when available
            CREATE TEMPORARY VIEW IF NOT EXISTS combined_urls AS
            SELECT
                school_name,
                base_hostname,
                CASE
                    WHEN redirected_url = '' OR redirected_url IS NULL THEN url_to_visit
                    ELSE redirected_url
                END AS url
            FROM crawl_queue_with_actual_urls;

            -- Gather the necessary info to create a new table
            CREATE TEMPORARY VIEW IF NOT EXISTS candidate_hostnames AS
            SELECT
                school_name,
                base_hostname,
                get_hostname_from_url(url) AS service_hostname
            FROM combined_urls
            GROUP BY school_name, base_hostname, service_hostname
            HAVING service_hostname <> '';

            -- Insert the data!
            INSERT INTO write_db.hostname (school_name, base_hostname, service_hostname)
            SELECT
                school_name,
                base_hostname,
                service_hostname
            FROM candidate_hostnames;

        """
        print('Initializing the hostname table...')
        await reader_conn.executescript(q)
        print('Done initializing the hostname table.')

    return (reader_conn, writer_conn)



def asyncify(func):
    """
    Decorator to turn a synchronous function into an asynchronous function
    using asyncio.run_in_executor.
    """

    @wraps(func)
    async def run(*args, **kwargs):
        loop = asyncio.get_running_loop()
        # Use run_in_executor to run the sync function in a separate thread
        # This prevents it from blocking the async loop
        return await loop.run_in_executor(None, lambda: func(*args, **kwargs))

    return run


@asyncify
def get_hostname_info_helper(hostname):

    records = fetch_dns_records(hostname)

    ptr_list = []
    for ip_addr in records['A']:
        ptr = fetch_ptr_record(ip_addr)
        ptr_list += ptr

    records['PTR'] = ptr_list

    return records



async def get_hostname_info(service_hostname, writer_conn, writer_lock):

    try:

        hostname_info = await get_hostname_info_helper(service_hostname)

        args = []
        for dns_info_type, dns_info in hostname_info.items():
            for record in dns_info:
                args.append((service_hostname, record, dns_info_type))

        q = """
            INSERT INTO hostname_dns_info (hostname, dns_info, dns_info_type)
            VALUES (?, ?, ?);
        """
        async with writer_lock:
            await writer_conn.executemany(q, args)

    except Exception as e:
        print(f"Error fetching DNS info for {service_hostname}: {e}. Traceback: {traceback.format_exc()}. Args: {args}")



def fetch_ptr_record(ip_address):
    # Configure the resolver to use the specified DNS server
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [DNS_SERVER]

    # Reverse the IP address and append .in-addr.arpa for IPv4 or .ip6.arpa for IPv6
    if ':' in ip_address:  # This is a very basic check for IPv6
        reversed_ip = '.'.join(reversed(ip_address.split(':'))) + '.ip6.arpa'
    else:
        reversed_ip = '.'.join(reversed(ip_address.split('.'))) + '.in-addr.arpa'

    # Fetch PTR record
    try:
        answer = resolver.resolve(reversed_ip, 'PTR')
        return [str(record) for record in answer]
    except Exception as e:
        return []


def fetch_dns_records(hostname):
    records = {}
    resolver = dns.resolver.Resolver()
    resolver.nameservers = [DNS_SERVER]

    # Fetch A records
    try:
        a_records = resolver.resolve(hostname, 'A')
        records['A'] = [ip.address for ip in a_records]
    except Exception as e:
        records['A'] = []

    # Fetch CNAME records
    try:
        cname_records = resolver.resolve(hostname, 'CNAME')
        records['CNAME'] = [cname.target.to_text() for cname in cname_records]
    except Exception as e:
        records['CNAME'] = []

    return records





if __name__ == '__main__':
    asyncio.run(main())