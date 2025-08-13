from hatchet_sdk import Hatchet, Context, ConcurrencyExpression, ConcurrencyLimitStrategy
from pydantic import BaseModel
from playwright.async_api import async_playwright
from pymongo import AsyncMongoClient

import settings

hatchet = Hatchet(debug=True)

class InputBook(BaseModel):
    url: str
    site: str
    book_id: int


topliba_com_workflow = hatchet.workflow(
    name='topliba-com-ltrs',
    on_events=['ltrs:topliba-book'],
    input_validator=InputBook,
    concurrency=ConcurrencyExpression(
        expression="'topliba-com'",
        max_runs=10,
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
    book = dict()
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
            'title': await page.text_content('h1'),
            'author': await page.text_content('h2.book-author'),
        }

        if links_download := await page.query_selector_all('a.download-btn'):
            book['links-download'] = [await l.get_attribute('href') for l in links_download]

        reader_site_locator = page.locator('a.read-btn')
        if await reader_site_locator.count() > 0:
            book['reader-site'] = await reader_site_locator.get_attribute('href')

        try:
            reader_litres_locator = page.locator('div.litres_fragment_body iframe')
            await reader_litres_locator.wait_for(state='attached', timeout=10_000)
            book['reader-litres'] = await reader_litres_locator.get_attribute('src')
        except:
            pass

        await context.close()

    client = AsyncMongoClient(settings.MONGO_URI)
    col = client['ltrs']['books']

    unique_key = {
        'book_id': input.book_id,
        'site': input.site,
        'url': input.url
    }

    data = unique_key | book

    # Обновляем документ или вставляем новый, если не существует
    await col.update_one(
        unique_key,
        {'$set': data},
        upsert=True
    )

    await client.aclose()

    return {'book': book}
