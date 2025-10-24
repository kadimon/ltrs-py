import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class DarkhorseComItem(BaseLivelibWorkflow):
    name = 'livelib-darkhorse-com-item'
    event = 'livelib:darkhorse-com-item'
    site='darkhorse.com'
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

        async with DbSamizdatPrisma() as db:
            if resp.status == 404:
                await db.mark_book_deleted(page.url, cls.site)
                return Output(result='error', data={'status': resp.status})

            await page.wait_for_selector('h2.title')

            book = {
                'url': page.url,
                'source': cls.site,
            }

            metrics = {
                'bookUrl': page.url,
            }

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('h2.title')
                await db.create_book(book)

            authors_locator = page.locator('.product_details dt').filter(
                has_text=re.compile(r'Writer:')
            ).locator('+ dd > a')
            if await authors_locator.count() > 0:
                book['author'] = ', '.join([await a.text_content() for a in await authors_locator.all()])
                # Все авторы с текстом и ссылками
                book['authors_data'] = []
                for a in await authors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['authors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            artists_locator = page.locator('.product_details dt').filter(
                has_text=re.compile(r'Artist:|Colorist:|Cover Artist:|Letterer:')
            ).locator('+ dd > a')
            if await artists_locator.count() > 0:
                # Все авторы с текстом и ссылками
                book['artists_data'] = []
                for a in await artists_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['artists_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            genres_locator = page.locator('.genre a')
            if await genres_locator.count() > 0:
                book['tags'] = [await g.text_content() for g in await genres_locator.all()]

            price_locator = page.locator('.product-meta dd').filter(
                has_text=re.compile(r'\$[\d+\.]+')
            )
            if await price_locator.count() > 0:
                metrics['price'] = re.search(r'[\d+\.]+', await price_locator.text_content())[0]

            pages_count_locator = page.locator('.product-meta dd').filter(
                has_text=re.compile(r'\d+\s+pages')
            )
            if await pages_count_locator.count() > 0:
                 metrics['pages_count'] = re.search(r'(\d+)\s+pages', await pages_count_locator.text_content()).group(1)

            age_rating_locator = page.locator('.product-meta dt').filter(
                has_text=re.compile(r'Age range:')
            ).locator('+ dd')
            if await age_rating_locator.count() > 0:
                 book['age_rating'] = re.search(r'\d+', await age_rating_locator.text_content())[0]

            date_release_locator = page.locator('.product-meta dt').filter(
                has_text=re.compile('Publication Date:')
             ).locator('+ dd')
            if await date_release_locator.count() > 0:
                 book['date_release'] = dateparser.parse(await date_release_locator.text_content())

            if annotation := await page.inner_text('.product-description'):
                book['annotation'] = annotation

            isbn_locator = page.locator('.product-meta dt').filter(
                has_text=re.compile('UPC:')
            ).locator('+ dd')
            if await isbn_locator.count() > 0:
                book['isbn'] = (await isbn_locator.text_content()).replace(' ', '')

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.product_main_image a', 'href', timeout=2_000):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                },
            )

class DarkhorseComListing(BaseLivelibWorkflow):
    name = 'livelib-darkhorse-com-listing'
    event = 'livelib:darkhorse-com-listing'
    site='darkhorse.com'
    input = InputLivelibBook
    output = Output

    concurrency=3
    execution_timeout_sec=300
    item_wf=DarkhorseComItem

    start_urls = [
        'https://www.darkhorse.com/Comics/Browse/January+1986-December+2026---0-Z/P5wfwkt8',
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

        if page.url in cls.start_urls:
            url_data = furl(page.url)

            last_page_num = await page.locator(
                    '[id^="go_to_page"] option'
                ).last.text_content()

            for page_num in range(2, int(last_page_num.strip())+1):
                url_data.args['page'] = page_num
                if await cls.crawl(url_data.url, input.task_id):
                    data['new-page-links'] += 1

        items_links = await page.query_selector_all('.list_items_container a.product_link:nth-child(1)')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await DarkhorseComItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )


if __name__ == '__main__':
    DarkhorseComListing.run_sync()

    # DarkhorseComListing.debug_sync(DarkhorseComListing.start_urls[0])
    DarkhorseComItem.debug_sync('https://www.darkhorse.com/Comics/3016-406/Captain-Henry-and-the-Graveyard-of-Time-1')
