import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class UnicomicsRuListing(BaseLivelibWorkflow):
    name = 'livelib-unicomics-ru-listing'
    event = 'livelib:unicomics-ru-listing'
    site='unicomics.ru'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300
    retries=10
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://unicomics.ru/comics/series/',
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
            'new-items-links': 0,
            'new-page-links': 0,
            'new-series-links': 0
        }

        pages_locator = page.locator('.paginator').first.locator('a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('a.list_title')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if '/series/' in item_url:
                if await UnicomicsRuListing.crawl(item_url, input.task_id):
                    data['new-series-links'] += 1
            elif '/issue/' in item_url:
                if await UnicomicsRuItem.crawl(item_url, input.task_id):
                    data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class UnicomicsRuItem(BaseLivelibWorkflow):
    name = 'livelib-unicomics-ru-item'
    event = 'livelib:unicomics-ru-item'
    site='unicomics.ru'
    input = InputLivelibBook
    output = Output

    @classmethod
    async def task(cls, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
            timeout=20_000,
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        await page.wait_for_selector('h1')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('h1')
                await db.create_book(book)

            title_original_locator = page.locator('.info h2')
            if await title_original_locator.count() > 0:
                book['title_original'] = await title_original_locator.text_content()

            owner_locator = page.locator('tr:has(.leftdescr)').filter(
                has_text=re.compile(r'Издательство:')
            ).locator('a')
            if await owner_locator.count() > 0:
                book['owner'] = await owner_locator.first.text_content()

            serie_locator = page.locator('tr:has(.leftdescr)').filter(
                has_text='Серия:|Серии:'
            ).locator('a')
            if await serie_locator.count() > 0:
                book['series'] = [await s.text_content() for s in await serie_locator.all()]

            lang_locator = page.locator('tr:has(.leftdescr)').filter(
                has_text=re.compile(r'Язык:')
            ).locator('td:nth-child(2)')
            if await lang_locator.count() > 0:
                book['language'] = await lang_locator.first.text_content()

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.image_comics img', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            await db.update_book(book)
            # await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )

if __name__ == '__main__':
    UnicomicsRuListing.run_sync()

    UnicomicsRuListing.debug_sync(UnicomicsRuListing.start_urls[0])
    UnicomicsRuItem.debug_sync('https://unicomics.ru/comics/issue/avengers-2018-marvel-001')
