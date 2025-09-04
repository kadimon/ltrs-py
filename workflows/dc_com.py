import re
from urllib.parse import urljoin
from pathlib import Path

from playwright.async_api import Page
import dateparser

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, InputEvent
from db import DbSamizdatPrisma
from utils import run_task, set_task, save_cover


class DcComListing(BaseLivelibWorkflow):
    name = 'livelib-dc-com-listing'
    event = 'livelib:dc-com-listing'
    input = InputLivelibBook
    output = Output

    execution_timeout_sec=60
    retries=10
    backoff_max_seconds=30
    backoff_factor=2

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

        data = {
            'items-links': 0,
            'page-links': 0,
        }

        if page.url == 'https://www.dc.com/comics':
            last_page_num = int(
                await page.locator(
                    'a[data-testid="pagination-navigation-button"]'
                ).last.text_content().strip()
            )
            for page_num in range(1, last_page_num+1):
                await set_task(InputEvent(
                    url=f'https://www.dc.com/comics?page={page_num}',
                    event=DcComListing.event,
                    site=input.site,
                    customer=self.customer,
                ))

                data['page-links'] += 1

        items_links = await page.query_selector_all('.resultsContainer .link-card a')
        for i in items_links:
            item_href = await i.get_attribute('href')
            item_url = urljoin(page.url, item_href)
            await set_task(InputEvent(
                url=item_url,
                event=DcComItem.event,
                site=input.site,
                customer=self.customer,
            ))

            data['items-links'] += 1

        if data['items-links'] == 0:
            raise Exception('ERROR: No Items')

        return Output(
            result='done',
            data=data,
        )

class DcComItem(BaseLivelibWorkflow):
    name = 'livelib-dc-com-item'
    event = 'livelib:dc-com-item'
    input = InputLivelibBook
    output = Output
    execution_timeout_sec=60

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

        await page.wait_for_selector('h1')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': input.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('h1')
                await db.create_book(book)

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

            artists_locator = page.locator('.list-values').filter(
                has_text=re.compile(r'Art by:|Cover:|Colorist:')
            ).locator('a')
            if await artists_locator.count() > 0:
                book['artist'] = await artists_locator.first.text_content()
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

            serie_locator = page.locator('.list-values').filter(
                has_text='Series:'
            ).locator('*[aria-label="list-values"]')
            if await serie_locator.count() > 0:
                book['series'] = [await serie_locator.text_content()]

            price_locator = page.locator('.list-values').filter(
                 has_text='U.S. Price:'
             ).locator('*[aria-label="list-values"]')
            if await price_locator.count() > 0:
                 metrics['price'] = price_locator.text_content()

            if pages_count := await page.locator('.list-values').filter(
                 has_text='Page Count:'
             ).locator('*[aria-label="list-values"]').text_content():
                 metrics['pages_count'] = pages_count

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
                    if img_name := await save_cover(page, img_src):
                        book['coverImage'] = img_name

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={'book': book, 'metrics': metrics},
            )


if __name__ == '__main__':
    # run_task(
    #     DcComListing,
    #     InputLivelibBook(
    #         url='https://www.dc.com/comics',
    #         site='dc.com'
    #     )
    # )

    run_task(
        DcComItem,
        InputLivelibBook(
            url='https://www.dc.com/comics/batman-fortress-2022/batman-fortress-4',
            site='dc.com'
        )
    )
