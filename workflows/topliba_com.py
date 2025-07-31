from hatchet_sdk import Hatchet, Context, ConcurrencyExpression, ConcurrencyLimitStrategy
from pydantic import BaseModel
from playwright.async_api import async_playwright
from pymongo import AsyncMongoClient

import settings

hatchet = Hatchet(debug=True)

class InputBook(BaseModel):
    url: str
    book_id: int


topliba_com_workflow = hatchet.workflow(
    name='topliba-com-ltrs',
    on_events=['ltrs:topliba-book'],
    input_validator=InputBook,
    concurrency=ConcurrencyExpression(
        expression='topliba-com',
        max_runs=2,
        limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    ),
)

@topliba_com_workflow.task(
    execution_timeout='30s',
    schedule_timeout='240h',
    retries=5,
    backoff_max_seconds=10,
    backoff_factor=2.0,
)
async def get_book(input: InputBook, ctx: Context):
    async with async_playwright() as p:
        context = await p.firefox.launch_persistent_context(
            './profileDir',
            proxy={'server': settings.PROXY_URI},
            headless=False,
            viewport={'width': 1920, 'height': 1080},
        )

        page = context.pages[0] if context.pages else await context.new_page()

        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        await page.wait_for_selector('h1')

        book = {
            'title': await page.text_content('h1.name'),
            'author': await page.text_content('h2.book-author'),
        }

        await context.close()

        return book

    # if not results:
    #     raise Exception('no results')

    # client = AsyncMongoClient(settings.MONGO_URI)
    # collection = client['ltrs']['yandex']

    # unique_key = {
    #     'book_id': input.book_id,
    #     'site': input.site,
    # }

    # data = unique_key | {'results': results}

    # # Обновляем документ или вставляем новый, если не существует
    # await collection.collection.update_one(
    #     unique_key,
    #     {'$set': data},
    #     upsert=True
    # )

    # await client.aclose()

    # return {'results': results}
