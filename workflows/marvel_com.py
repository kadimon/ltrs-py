import re
from urllib.parse import urljoin
from pathlib import Path

from playwright.async_api import Page
import dateparser
from furl import furl

from workflow_base import BaseLivelibWorkflow
from interfaces import InputLivelibBook, Output, InputEvent
from db import DbSamizdatPrisma
from utils import run_task, set_task, save_cover, set_task_sync


class MarvelComListing(BaseLivelibWorkflow):
    name = 'livelib-marvel-com-listing'
    event = 'livelib:marvel-com-listing'
    input = InputLivelibBook
    output = Output

    # proxy_enable = False

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

        page_data = await resp.json()

        page_url = furl(page.url)

        if page_url.args['offset'] == '0':
            total_books = page_data['data']['total']
            for offset in range(1000, total_books, 1000):
                page_url.args['offset'] = offset
                await set_task(InputEvent(
                    url=page_url.tostr(),
                    event=MarvelComListing.event,
                    site=input.site,
                    customer=self.customer,
                ))

                data['page-links'] += 1

        for i in page_data['data']['results']:
            await set_task(InputEvent(
                url=i['metadata']['url'],
                event=MarvelComItem.event,
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

class MarvelComItem(BaseLivelibWorkflow):
    name = 'livelib-marvel-com-item'
    event = 'livelib:marvel-com-item'
    input = InputLivelibBook
    output = Output

    # proxy_enable = False
    execution_timeout_sec=300

    async def task(self, input: InputLivelibBook, page: Page) -> Output:
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

        await page.wait_for_selector('.ComicMasthead__Title h1.ModuleHeader')

        async with DbSamizdatPrisma() as db:
            book = {
                'url': page.url,
                'source': input.site,
            };

            metrics = {
                'bookUrl': page.url,
            };

            if not await db.check_book_exist(page.url):
                book['title'] = await page.text_content('.ComicMasthead__Title h1.ModuleHeader')
                await db.create_book(book)

            authors_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                has_text=re.compile(r'Writer:')
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

            artists_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                has_text=re.compile(r'Penciller:|Inker:|Colorist:|Letterer:|Cover Artist:|Inker (Cover):|Colorist (Cover):')
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

            editors_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                has_text=re.compile(r'Editor:')
            ).locator('a')
            if await editors_locator.count() > 0:
                # Все авторы с текстом и ссылками
                book['editors_data'] = []
                for a in await editors_locator.all():
                    href = await a.get_attribute('href')
                    text = await a.text_content()
                    absolute_url = urljoin(page.url, href)

                    book['editors_data'].append({
                        'name': text.strip(),
                        'url': absolute_url
                    })

            # serie_locator = page.locator('.list-values').filter(
            #     has_text='Series:'
            # ).locator('*[aria-label="list-values"]')
            # if await serie_locator.count() > 0:
            #     book['series'] = [await serie_locator.text_content()]

            isbn_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                 has_text='UPC:'
             ).locator('span:nth-child(2)')
            if await isbn_locator.count() > 0:
                 metrics['isbn'] = await isbn_locator.text_content()

            age_rating_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                 has_text='Rating:'
             ).locator('span:nth-child(2)')
            if await age_rating_locator.count() > 0:
                 metrics['age_rating_str'] = await age_rating_locator.text_content()

            price_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                 has_text='Price:'
             ).locator('span:nth-child(2)')
            if await price_locator.count() > 0:
                 metrics['price'] = await price_locator.text_content()

            pages_count_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                 has_text='Page Count:'
             ).locator('span:nth-child(2)')
            if await pages_count_locator.count() > 0:
                 metrics['pages_count'] = await pages_count_locator.text_content()

            date_release_locator = page.locator('.ComicIssueMoreDetails__List li').filter(
                 has_text='FOC Date:'
             ).locator('span:nth-child(2)')
            if await date_release_locator.count() > 0:
                release_date_str = await date_release_locator.text_content()
                book['date_release'] = dateparser.parse(release_date_str)

            if annotation := await page.text_content('.ComicMasthead__Description'):
                book['annotation'] = annotation

            if artwork_type := await page.locator('.ComicIssueMoreDetails__List li').filter(
                 has_text='Format:'
             ).locator('span:nth-child(2)').text_content():
                book['artwork_type'] = artwork_type

            if not await db.check_book_have_cover(page.url):
                if img_src := await page.get_attribute('.ComicMasthead__ImageWrapper img', 'src'):
                    if img_name := await save_cover(page, img_src, timeout=20_000):
                        book['coverImage'] = img_name

            await db.update_book(book)
            await db.create_metrics(metrics)

            return Output(
                result='done',
                data={'book': book, 'metrics': metrics},
            )

start_urls = [
    'https://bifrost.marvel.com/v1/catalog/comics/calendar/?byType=date&offset=0&limit=1000&orderBy=release_date%2Bdesc%2Ctitle%2Basc&variants=false&formatType=issue&dateStart=1820-12-31&dateEnd=2028-12-31',
]

if __name__ == '__main__':
    for url in start_urls:
        set_task_sync(
            InputEvent(
                url=url,
                event=MarvelComListing.event,
                site='marvel.com',
                customer=MarvelComListing.customer,
                dedupe_hours=0,
            )
        )

    run_task(
        MarvelComListing,
        InputLivelibBook(
            url=start_urls[0],
            site='marvel.com'
        )
    )

    run_task(
        MarvelComItem,
        InputLivelibBook(
            url='https://www.marvel.com/comics/issue/5538/new_excalibur_2005_13',
            site='marvel.com'
        )
    )
