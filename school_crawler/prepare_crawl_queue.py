"""
Initializes the crawl queue.

Usage: python prepare_crawl_queue.py <input_csv> <db_path>

"""
import pandas
import sys
import asyncio
import httpx
import aiosqlite
from tqdm import tqdm
import pandas as pd


NUMBER_OF_WORKERS = 15

SCHOOL_INFO_TABLE_SCHEMA = """
    CREATE TABLE IF NOT EXISTS school_info (
        school_name TEXT,
        nces_website TEXT,
        actual_website TEXT,
        actual_website_status TEXT
    );
    CREATE INDEX IF NOT EXISTS nces_website_index ON school_info (nces_website);
"""

HTTP_CLIENT_CONFIG = {
    'follow_redirects': True,
    'headers': {
        'Referer': 'https://www.google.com/',
        'User-Agent': 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36'
    }
}


progress_dict = {'previous_pending_count': None, 'pbar': None}


def main():

    # Read the input CSV from the command line
    try:
        input_csv = sys.argv[1]
        db_path = sys.argv[2]
    except IndexError:
        print("Usage: python prepare_crawl_queue.py <input_csv> <db_path>")
        sys.exit(1)

    asyncio.run(start_processing(input_csv, db_path))


async def get_db_itr(db, q, args=[]):
    async with db.execute(q, args) as cursor:
        async for row in cursor:
            yield row


async def db_fetch_all(db, q, args=[]):
    async with db.execute(q, args) as cursor:
        rows = await cursor.fetchall()

    return rows


async def worker(worker_id, client, db):

    # Find schools where the actual_website_status is pending or previously
    # crawled but unsuccessful
    q = """
        SELECT nces_website, MIN(rowid) as min_id
        FROM school_info
        WHERE actual_website_status != '200'
        GROUP BY nces_website
        HAVING MOD(min_id, ?) = ?
        ORDER BY RANDOM();
    """
    async for row in get_db_itr(db, q, (NUMBER_OF_WORKERS, worker_id)):

        nces_website = row['nces_website']

        # Visit the site to get the redirected URL
        try:
            resp = await client.get(nces_website)
            actual_website = str(resp.url)
            actual_website_status = resp.status_code
        except Exception as e:
            actual_website = None
            actual_website_status = 'error'

        # Update the database
        q = """
            UPDATE school_info
            SET actual_website = ?,
                actual_website_status = ?
            WHERE
                nces_website = ? AND
                actual_website_status = 'pending'
        """
        await db.execute(q, (actual_website, actual_website_status, nces_website))

        # Print out statistics
        if worker_id == 0:
            q = """
                SELECT count(*) AS pending_count
                FROM school_info
                WHERE actual_website_status != '200'
            """
            async for row in get_db_itr(db, q):
                pending_count = row['pending_count']
                if progress_dict['previous_pending_count'] is None:
                    progress_dict['previous_pending_count'] = pending_count
                    progress_dict['pbar'] = tqdm(total=pending_count)
                else:
                    progress_dict['pbar'].update(progress_dict['previous_pending_count'] - pending_count)
                    progress_dict['previous_pending_count'] = pending_count


async def initialize_db(input_csv, db):

    # Read the input CSV and extract the `SCH_NAME` and `WEBSITE` columns
    print('Reading input CSV...')
    df = pandas.read_csv(input_csv)
    df = df[['SCH_NAME', 'WEBSITE']]

    # Remove any empty values
    df = df[df['SCH_NAME'].notnull() & df['WEBSITE'].notnull()].drop_duplicates()

    # Create the schema
    await db.executescript(SCHOOL_INFO_TABLE_SCHEMA)

    # Insert contents from the dataframe
    print('Processing the dataframe...')
    school_name_website_list = []
    for (_, row) in df.iterrows():
        school_name_website_list.append((row['SCH_NAME'], row['WEBSITE']))

    print('Inserting school info into the database...')
    await db.executemany("""
        INSERT INTO school_info (school_name, nces_website, actual_website_status) VALUES (?, ?, 'pending')
        """, school_name_website_list)




async def start_processing(input_csv, db_path):

    tasks = []

    async with aiosqlite.connect(db_path, isolation_level=None) as db:

        db.row_factory = aiosqlite.Row

        # Check if we have loaded school info previously
        try:
            await db.execute("SELECT * FROM school_info")

        # Looks like we don't know about the schools; parse the input CSV and load the data
        except aiosqlite.OperationalError:
            await initialize_db(input_csv, db)

        # Start the crawl workers
        print('Starting the crawl workers...')
        async with httpx.AsyncClient(**HTTP_CLIENT_CONFIG) as client:
            for worker_id in range(NUMBER_OF_WORKERS):
                task = asyncio.create_task(worker(worker_id, client, db))
                tasks.append(task)

            await asyncio.gather(*tasks, return_exceptions=False)

        # Clean up the pbar
        if progress_dict['pbar'] is not None:
            progress_dict['pbar'].close()

        # Show final statistics
        q = """
            SELECT actual_website_status, COUNT(*) AS c
            FROM school_info
            GROUP BY actual_website_status
            ORDER BY c DESC
        """
        rows = await db_fetch_all(db, q)
        df = pd.DataFrame(rows, columns=['actual_website_status', 'count'])
        print(df.to_string())

    print('Done!')



if __name__ == "__main__":
    main()