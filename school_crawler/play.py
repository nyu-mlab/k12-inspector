import asyncio
import random
import httpx


async def worker(name, session):

    print(f'{name} is starting')
    for i in range(5):
        try:
            resp = await session.get(f'http://bjhs.madisoncity.k12.al.us')
            print(f'{name}.{i} got response {resp.status_code}: {resp.url} - {type(resp.url)}')
        except Exception as e:
            print(f'{name}.{i} got exception {e}')



    print(f'{name} is done')


async def main():

    # Create three worker tasks to process the queue concurrently.
    tasks = []
    async with httpx.AsyncClient(follow_redirects=True) as session:
        for i in range(3):
            task = asyncio.create_task(worker(f'worker-{i}', session))
            tasks.append(task)

        await asyncio.gather(*tasks, return_exceptions=False)

    print('Done')

asyncio.run(main())