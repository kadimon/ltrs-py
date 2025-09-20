import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class WebcomicsappComListing(BaseLivelibWorkflow):
    name = 'livelib-webcomicsapp-com-listing'
    event = 'livelib:webcomicsapp-com-listing'
    site='webcomicsapp.com'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300

    start_urls = [
        'https://www.webcomicsapp.com/genres/All/All/Popular/1',
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
        }

        pages_locator = page.locator('.page-list a')
        for page_locator in await pages_locator.all():
            if await cls.crawl(
                urljoin(page.url, await page_locator.get_attribute('href')),
                input.task_id,
            ):
                data['new-page-links'] += 1

        items_links = await page.query_selector_all('.list-item > a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await WebcomicsappComItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class WebcomicsappComItem(BaseLivelibWorkflow):
    name = 'livelib-webcomicsapp-com-item'
    event = 'livelib:webcomicsapp-com-item'
    site='webcomicsapp.com'
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

        await page.wait_for_selector('.info > h5')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': cls.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('.info > h5')
                await db.create_book(book)

            authors_str_locator = page.locator('.author')
            if await authors_str_locator.count() > 0:
                book['author'] = (await authors_str_locator.text_content()).replace('/', ', ')

            genres_locator = page.locator('a.label-tag')
            if await genres_locator.count() > 0:
                book['tags'] = [await g.text_content() for g in await genres_locator.all()]

            if annotation := await page.text_content('.perjury'):
                book['annotation'] = annotation

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('img.pc-book-img', 'src', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            comments_count = await page.locator('.wpd-thread-info').count()
            if comments_count > 0:
                metrics['comments'] = comments_count

            likes_locator = page.locator('.counts-icon:has(.icon-hot)')
            if await likes_locator.count() > 0:
                metrics['likes'] = await likes_locator.first.text_content()

            added_to_lib_locator = page.locator('.counts-icon:has(.icon-like)')
            if await added_to_lib_locator.count() > 0:
                metrics['added_to_lib'] = await added_to_lib_locator.first.text_content()

            chapters_count = await page.locator('.chapter-item-cover').count()
            if chapters_count > 0:
                metrics['chapters_count'] = chapters_count

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
    WebcomicsappComListing.run_sync()

    WebcomicsappComListing.debug_sync(WebcomicsappComListing.start_urls[0])
    WebcomicsappComItem.debug_sync('https://www.webcomicsapp.com/comic/Adore-Me-Exclusively/61934ce08c252b2cf46d1d07')
