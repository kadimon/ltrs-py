import logging

from playwright.async_api import Page
from pymongo import AsyncMongoClient
from hatchet_sdk import Hatchet, ClientConfig, PushEventOptions, V1TaskStatus

from workflow_base import BaseLtrsSeWorkflow
from interfaces import InputSeLtrs, Output
import settings

root_logger = logging.getLogger('hatchet')
root_logger.setLevel(logging.WARNING)

hatchet = Hatchet(
    debug=False,
    config=ClientConfig(
        logger=root_logger,
    ),
)


class YandexLtrs(BaseLtrsSeWorkflow):
    name = 'yandex-positions-ltrs'
    event = 'ltrs:yandex'
    site='ya.ru'
    input = InputSeLtrs
    output = Output

    sources = [
        'topliba.com',
        'knigavuhe.org',
        'readli.net',
        'flibusta.su',
        'avidreaders.ru',
        'flibusta.one',
        'yakniga.org',
        'libcat.ru',
        'audiokniga-one.com',
        'librebook.me',
    ]

    @classmethod
    async def task(cls, input: InputSeLtrs, page: Page) -> Output:
        await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        results = []
        for r_num, r in enumerate(await page.locator('div.OrganicTitle').all(), 1):
            r_link = r.locator('a')

            results.append({
                'num': r_num,
                'text': await r.text_content(),
                'url': await r_link.get_attribute('href'),
            })

        if not results:
            raise Exception('no results')

        client = AsyncMongoClient(settings.MONGO_URI)
        col = client['ltrs']['yandex']

        unique_key = {
            'book_id': input.book_id,
            'source': input.source,
        }

        data = unique_key | {'results': results}

        # Обновляем документ или вставляем новый, если не существует
        await col.update_one(
            unique_key,
            {'$set': data},
            upsert=True
        )

        await client.aclose()

        return Output(
            result='done',
            data={
                'positions': results
            }
        )

if __name__ == '__main__':
    YandexLtrs.run_sync()

    source = YandexLtrs.sources[0]
    query = 'Донцова'
    YandexLtrs.debug_sync(
        f'https://ya.ru/search/?text=site:{source}+{query}&lr=225',
        source=source,
        query=query,
        book_id=0,
    )
