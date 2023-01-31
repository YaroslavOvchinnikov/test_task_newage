from io import BytesIO
from PIL import Image
import httpx
import asyncio
import pandas as pd
import gspread
from gspread_dataframe import set_with_dataframe

service_account = gspread.service_account(filename='file.json')
# read
read_sheet = service_account.open_by_url("https://docs.google.com/spreadsheets/d/1QX2IhFyYmGDFMvovw2WFz3wAT4piAZ_8hi5Lzp7LjV0/edit#gid=1902149593")
rows = read_sheet.sheet1.get_all_records()
# write
work_sheet = service_account.open("1")

photo_data = []

client = httpx.AsyncClient()
sem = asyncio.Semaphore(50)

async def get_size(url):
    try:
        r = await client.get(url)
        image_bytes = r.content
        image_data = Image.open(BytesIO(image_bytes))
        width, height = image_data.size
        photo_parametrs = {
            "url": url,
            "width": width,
            "height": height
        }
        return photo_parametrs
    except:
        pass


async def bound_fetch(url, photo_data):
    async with sem:
        photo_parametrs = await get_size(url)
        if photo_parametrs != None:
            photo_data.append(photo_parametrs)
        else:
            print(url, 'cannot get size or another problem')


async def main(rows):
    tasks_list = []
    for i in range(1, len(rows)):
        task = asyncio.ensure_future(bound_fetch(rows[i]['image_url'], photo_data))
        tasks_list.append(task)
    return await asyncio.gather(*tasks_list)

loop = asyncio.get_event_loop()
loop.run_until_complete(main(rows))


def write_data(photo_data):
    work_sheet.sheet1.clear()
    df = pd.DataFrame.from_dict(photo_data, orient='columns')
    set_with_dataframe(work_sheet.sheet1, df, include_column_header=True)
    return 1

write_data(photo_data)