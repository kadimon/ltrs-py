from hatchet_sdk import Hatchet, Context, ConcurrencyExpression, ConcurrencyLimitStrategy
from pydantic import BaseModel
from playwright.async_api import async_playwright
from pymongo import AsyncMongoClient

import settings

hatchet = Hatchet(debug=True)

class InputYandexLtrs(BaseModel):
    site: str
    query: str
    book_id: int


yandex_ltrs_workflow = hatchet.workflow(
    name='yandex-positions-ltrs',
    on_events=['ltrs:yandex'],
    input_validator=InputYandexLtrs,
    concurrency=ConcurrencyExpression(
        expression='yandex-positions-ltrs',
        max_runs=2,
        limit_strategy=ConcurrencyLimitStrategy.GROUP_ROUND_ROBIN,
    ),
)

@yandex_ltrs_workflow.task(
    execution_timeout='30s',
    schedule_timeout='240h',
    retries=5,
    backoff_max_seconds=10,
    backoff_factor=2.0,
)
async def get_positions(input: InputYandexLtrs, ctx: Context):
    results = []

    async with async_playwright() as p:
        context = await p.firefox.launch_persistent_context(
            './profileDir',
            proxy={'server': settings.PROXY_URI},
            headless=False,
            viewport={'width': 1920, 'height': 1080},
        )

        page = context.pages[0] if context.pages else await context.new_page()

        await page.goto(
            f'https://ya.ru/search/?text=site:{input.site}+{input.query}&lr=225',
            wait_until='domcontentloaded',
        )

        results_locator = page.locator('div.OrganicTitle')

        for r_num in range(await results_locator.count()):
            r = results_locator.nth(r_num)

            r_link = r.locator('a')

            results.append({
                'num': r_num + 1,
                'text': await r.text_content(),
                'url': await r_link.get_attribute('href'),
            })

        await context.close()

    if not results:
        raise Exception('no results')

    client = AsyncMongoClient(settings.MONGO_URI)
    collection = client['ltrs']['yandex']

    unique_key = {
        'book_id': input.book_id,
        'site': input.site,
    }

    data = unique_key | {'results': results}

    # Обновляем документ или вставляем новый, если не существует
    await collection.collection.update_one(
        unique_key,
        {'$set': data},
        upsert=True
    )

    await client.aclose()

    return result
