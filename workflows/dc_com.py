import re
from urllib.parse import urljoin

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, WorkerLabels
from db import DbSamizdatPrisma
from utils import save_cover


class DcComListing(BaseLivelibWorkflow):
    name = 'livelib-dc-com-listing'
    event = 'livelib:dc-com-listing'
    site='dc.com'
    input = InputLivelibBook
    output = Output

    execution_timeout_sec=600
    retries=10
    backoff_max_seconds=30
    backoff_factor=2

    start_urls = [
        'https://www.dc.com/comics',
        'https://www.dc.com/comics?sort=eyJvcmRlciI6ImFzYyIsImZpZWxkIjoiY3JlYXRlZCJ9',
        'https://www.dc.com/comics?sort=eyJvcmRlciI6ImFzYyIsImZpZWxkIjoidGl0bGUifQ%3D%3D',
        'https://www.dc.com/comics?sort=eyJvcmRlciI6ImRlc2MiLCJmaWVsZCI6InRpdGxlIn0%3D',
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
                    'a[data-testid="pagination-navigation-button"]'
                ).last.text_content()
            for page_num in range(1, int(last_page_num.strip())+1):
                url_data.args['page'] = page_num
                if await cls.crawl(url_data.url, input.task_id):
                    data['new-page-links'] += 1

        items_links = await page.query_selector_all('.resultsContainer .link-card a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await DcComItem.crawl(item_url, input.task_id):
                data['new-items-links'] += 1

        if not items_links:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class DcComItem(BaseLivelibWorkflow):
    name = 'livelib-dc-com-item'
    event = 'livelib:dc-com-item'
    site='dc.com'
    input = InputLivelibBook
    output = Output

    labels = WorkerLabels(ip='rs')

    # proxy_enable = False
    execution_timeout_sec=60

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

            persons_urls = []

            authors_locator = page.locator('.list-values').filter(
                has_text=re.compile(r'Writer:|Written by:')
            ).locator('a')
            if await authors_locator.count() > 0:
                book['author'] = await authors_locator.first.text_content()
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

                    persons_urls.append(absolute_url)

            artists_locator = page.locator('.list-values').filter(
                has_text=re.compile(r'Art by:|Cover:|Colorist:')
            ).locator('a')
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

                    persons_urls.append(absolute_url)

            serie_locator = page.locator('.list-values').filter(
                has_text='Series:'
            ).locator('*[aria-label="list-values"]')
            if await serie_locator.count() > 0:
                book['series'] = [await serie_locator.text_content()]

            price_locator = page.locator('.list-values').filter(
                 has_text='U.S. Price:'
             ).locator('*[aria-label="list-values"]')
            if await price_locator.count() > 0:
                 metrics['price'] = await price_locator.text_content()

            if pages_count := await page.locator('.list-values').filter(
                 has_text='Page Count:'
             ).locator('*[aria-label="list-values"]').text_content():
                 metrics['pages_count'] = pages_count

            if age_rating := await page.locator('.list-values').filter(
                 has_text='Rated:'
             ).locator('*[aria-label="list-values"]').text_content():
                 book['age_rating_str'] = age_rating

            if date_release := await page.locator('.list-values').filter(
                 has_text='On Sale Date:'
             ).locator('*[aria-label="list-values"]').text_content():
                 book['date_release'] = dateparser.parse(date_release)

            if annotation := await page.text_content('div[data-testid="textContainer"] > div > p:nth-child(2)'):
                book['annotation'] = annotation

            if artwork_type := await page.text_content('p:has(~h1)'):
                book['artwork_type'] = artwork_type

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('article > section:nth-child(2) img', 'src'):
                    if img_name := await save_cover(page, img_src, timeout=10_000):
                        book['coverImage'] = img_name

            await db.update_book(book)
            await db.create_metrics(metrics)

            new_persons = 0
            for p_url in persons_urls:
                if await DcComPerson.crawl(p_url, input.task_id):
                    new_persons += 1

            return Output(
                result='done',
                data={
                    'book': book,
                    'metrics': metrics,
                    'new-persons': new_persons,
                },
            )

class DcComPerson(BaseLivelibWorkflow):
    name = 'livelib-dc-com-person'
    event = 'livelib:dc-com-person'
    site='dc.com'
    input = InputLivelibBook
    output = Output

    execution_timeout_sec = 240

    async def task(self, input: InputLivelibBook, page: Page) -> Output:
        resp = await page.goto(
            input.url,
            wait_until='domcontentloaded',
        )
        if not (200 <= resp.status < 400):
            return Output(
                result='error',
                data={'status': resp.status},
            )

        items_links = await page.query_selector_all('.grid-items a')
        new_items = 0
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            if await DcComItem.crawl(item_url, input.task_id):
                new_items += 1


        return Output(
            result='done',
            data={'new-items': new_items},
        )


if __name__ == '__main__':
    DcComListing.run_sync()

    DcComListing.debug_sync(DcComListing.start_urls[0])
    DcComItem.debug_sync('https://www.dc.com/comics/batman-fortress-2022/batman-fortress-4')
