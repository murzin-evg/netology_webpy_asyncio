import asyncio
import datetime

import aiohttp
from more_itertools import chunked

from models import Base, Session, SWAPI, engine

MAX_REQUESTS_CHUNK = 5


async def get_person_data(person):
    keys = [
        'birth_year',
        'eye_color',
        'films',
        'gender',
        'hair_color',
        'height',
        'homeworld',
        'mass',
        'name',
        'skin_color',
        'species',
        'starships',
        'vehicles'
    ]

    result = {}
    for key in keys:
        try:
            result.update({key: str(person[key])})
        except KeyError:
            result.update({key: None})
    return result


async def insert_people(people_list_json):
    people_list = [SWAPI(**person) for person in people_list_json]
    async with Session() as session:
        session.add_all(people_list)
        await session.commit()


async def get_people(people_id):
    session = aiohttp.ClientSession()
    response = await session.get(f"https://swapi.py4e.com/api/people/{people_id}")
    json_data = await response.json()
    final_data = await get_person_data(json_data)
    await session.close()
    return final_data


async def main():
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)

    for person_ids_chunk in chunked(range(1, 100), MAX_REQUESTS_CHUNK):
        person_coros = [get_people(person_id) for person_id in person_ids_chunk]
        people = await asyncio.gather(*person_coros)
        insert_people_coro = insert_people(people)
        asyncio.create_task(insert_people_coro)

    main_task = asyncio.current_task()
    insets_tasks = asyncio.all_tasks() - {main_task}
    await asyncio.gather(*insets_tasks)

    print("Success")


if __name__ == "__main__":
    start = datetime.datetime.now()
    asyncio.run(main())
    print(datetime.datetime.now() - start)
