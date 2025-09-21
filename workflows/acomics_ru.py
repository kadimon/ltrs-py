import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


TOTAL_ITEMS = 2900

class AcomicsRuListing(BaseLivelibWorkflow):
    name = 'livelib-acomics-ru-listing'
    event = 'livelib:acomics-ru-listing'
    site='acomics.ru'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        f'https://acomics.ru/comics?skip={offset}'
        for offset in range(0, TOTAL_ITEMS, 10)
    ]

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )

        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        data = {
            'updated-items': 0,
            'new-nav-links': 0,
        }

        async with DbSamizdatPrisma() as db:
            for item in await page.locator('.serial-card').all():
                title_locator = item.locator('.title a:first-of-type')
                item_url = urljoin(page.url, await title_locator.get_attribute('href'))

                book = {
                    'url': item_url,
                    'source': cls.site,
                };

                metrics = {
                    'bookUrl': item_url,
                };

                if not await db.check_book_exist(item_url):
                    book['title'] = await title_locator.text_content()
                    await db.create_book(book)

                annotation_locator = item.locator('.about')
                if await annotation_locator.count() > 0:
                    book['annotation'] = await annotation_locator.first.text_content()

                age_rating_locator = item.locator('.age-rating a')
                if await age_rating_locator.count() > 0:
                     book['age_rating_str'] = await age_rating_locator.text_content()

                if not await db.check_book_have_cover(item_url):
                    if img_src := await item.locator(
                        '.cover img'
                    ).get_attribute('src', timeout=2_000):
                        if img_name := await save_cover(page, img_src, timeout=10_000):
                            book['coverImage'] = img_name

                content_update_date_locator = item.locator('.date-time-formatted')
                if await content_update_date_locator.count() > 0:
                    content_update_date_str = await content_update_date_locator.text_content()
                    metrics['content_update_date'] = dateparser.parse(content_update_date_str)

                added_to_lib_locator = item.locator('.subscr-count')
                if await added_to_lib_locator.count() > 0:
                    metrics['added_to_lib'] = await added_to_lib_locator.first.text_content()

                chapters_count_locator = item.locator('.issue-count')
                if await chapters_count_locator.count() > 0:
                    metrics['chapters_count'] = re.search(r'\d+', await chapters_count_locator.text_content())[0]

                await db.update_book(book)
                await db.create_metrics(metrics)
                data['updated-items'] += 1

        return Output(
            result='done',
            data=data,
        )

if __name__ == '__main__':
    AcomicsRuListing.run_sync()

    AcomicsRuListing.debug_sync(AcomicsRuListing.start_urls[0])
