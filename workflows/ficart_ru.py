import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class FicartRuListing(BaseLivelibWorkflow):
    name = 'livelib-ficart-ru-listing'
    event = 'livelib:ficart-ru-listing'
    site='ficart.ru'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300

    start_urls = [
        'https://ficart.ru/fanfic/',
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

        pages_locator = page.locator('.navigation a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('.argmore a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if '/series/' in item_url:
                if await FicartRuListing.crawl(item_url, input.task_id):
                    data['new-series-links'] += 1
            elif '/issue/' in item_url:
                if await FicartRuItem.crawl(item_url, input.task_id):
                    data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class FicartRuItem(BaseLivelibWorkflow):
    name = 'livelib-ficart-ru-item'
    event = 'livelib:ficart-ru-item'
    site='ficart.ru'
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

            publisher_locator = page.locator('tr:has(.leftdescr)').filter(
                has_text=re.compile(r'Издательство:')
            ).locator('a')
            if await publisher_locator.count() > 0:
                book['publisher'] = await publisher_locator.first.text_content()

            serie_locator = page.locator('tr:has(.leftdescr)').filter(
                has_text=re.compile('Серия:|Серии:')
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

            likes_locator = page.frame_locator('#vkwidget2').locator('#stats_num')
            await likes_locator.wait_for(state='visible')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.text_content()

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )

if __name__ == '__main__':
    FicartRuListing.run_sync()

    FicartRuListing.debug_sync(FicartRuListing.start_urls[0])
    FicartRuItem.debug_sync('https://ficart.ru/comics/issue/avengers-2018-marvel-001')
